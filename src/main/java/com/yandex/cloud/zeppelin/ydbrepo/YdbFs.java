package com.yandex.cloud.zeppelin.ydbrepo;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructValue;

/**
 * Basic CRUD operations for files and folders stored in YDB.
 * 
 * @author mzinal
 */
public class YdbFs implements AutoCloseable {

    private final String basePath;
    private final String database;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryContext;
    private final Queries query;

    /**
     * Initializing constructor.
     *
     * @param url Database connection URL as `grpcs://host:port?database=/Root/dbname`
     * @param authMode Authentication mode
     * @param authData Authentication data, depending on the mode chosen
     * @param baseDir Database schema for the tables
     */
    public YdbFs(String url, AuthMode authMode, String authData, String baseDir) {
        GrpcTransportBuilder gtb = GrpcTransport.forConnectionString(url);
        switch (authMode) {
            case NONE:
                break;
            case METADATA:
                gtb.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
                break;
            case STATIC:
                gtb.withAuthProvider(makeStaticAuthProvider(authData));
                break;
            case SAKEY:
                gtb.withAuthProvider(CloudAuthHelper.getServiceAccountFileAuthProvider(authData));
                break;
        }
        transport = gtb.build();
        database = transport.getDatabase();
        tableClient = TableClient.newClient(transport).sessionPoolSize(0, 16).build();
        retryContext = SessionRetryContext.create(tableClient).build();
        this.basePath = (baseDir==null || baseDir.length()==0) ?
                database : (database + "/" + baseDir);
        this.query = new Queries(this.basePath);
    }

    @Override
    public void close() {
        if (tableClient!=null) {
            try {
                tableClient.close();
            } catch(Throwable t) {}
        }
        if (transport!=null) {
            try {
                transport.close();
            } catch(Throwable t) {}
        }
    }

    /**
     * List all folders stored
     *
     * @return Mapping folder ids to folder descriptions.
     */
    public Map<String, Folder> listFolders() {
        final Map<String, Folder> m = new HashMap<>();
        final ReadTableSettings settings = ReadTableSettings.newBuilder()
            .orderedRead(false).columns("did", "dparent", "dname").build();
        try (Session session = tableClient.createSession(Duration.ofSeconds(30)).join().getValue()) {
            session.readTable(basePath + "/zdir", settings, rs -> {
                final int did = rs.getColumnIndex("did");
                final int dparent = rs.getColumnIndex("dparent");
                final int dname = rs.getColumnIndex("dname");
                while (rs.next()) {
                    Folder f = new Folder(
                            rs.getColumn(did).getText(),
                            rs.getColumn(dparent).getText(),
                            rs.getColumn(dname).getText());
                    m.put(f.id, f);
                }
            });
        }
        return m;
    }

    /**
     * List all files used.
     *
     * @return Mapping file ids to file descriptions.
     */
    public Map<String, File> listFiles() {
        final Map<String, File> m = new HashMap<>();
        final ReadTableSettings settings = ReadTableSettings.newBuilder()
            .orderedRead(false).columns("fid", "fparent", "fname").build();
        try (Session session = tableClient.createSession(Duration.ofSeconds(30)).join().getValue()) {
            session.readTable(basePath + "/zfile", settings, rs -> {
                final int fid = rs.getColumnIndex("fid");
                final int fparent = rs.getColumnIndex("fparent");
                final int fname = rs.getColumnIndex("fname");
                final int vid = rs.getColumnIndex("vid");
                while (rs.next()) {
                    File f = new File(
                            rs.getColumn(fid).getText(),
                            rs.getColumn(fparent).getText(),
                            rs.getColumn(fname).getText(),
                            rs.getColumn(vid).getText());
                    m.put(f.id, f);
                }
            });
        }
        return m;
    }

    /**
     * Obtain the complete listing of all objects, including files and folders.
     *
     * @return Files and folders descriptions and their hierarchy.
     */
    public FullList listAll() {
        return new FullList(listFolders(), listFiles()).fill();
    }

    /**
     * Read the current file's data.
     *
     * @param id File id
     * @return File data, or null if the file does not exist.
     */
    public byte[] readFile(String id) {
        final List<byte[]> data = new ArrayList<>();
        final PrimitiveValue vid = PrimitiveValue.newText(id);
        final int limit = 10;
        final PrimitiveValue vlimit = PrimitiveValue.newInt32(limit);

        int totalRows = retryContext.supplyResult(session -> {
            final TxControl<?> tx = TxControl.onlineRo();
            final int[] pos = new int[] { -1 };
            int count, totalCount = 0;
            do {
                Params params = Params.of("$fid", vid, "$limit", vlimit,
                        "$pos", PrimitiveValue.newInt32(pos[0]));
                count = session.executeDataQuery(query.readFile, tx, params)
                        .thenApply(Result::getValue)
                        .thenApply(result -> {
                            int localCount = 0;
                            ResultSetReader rsr = result.getResultSet(0);
                            while (rsr.next()) {
                                data.add(rsr.getColumn(0).getBytes());
                                int curPos = rsr.getColumn(1).getInt32();
                                if (curPos > pos[0])
                                    pos[0] = curPos;
                                ++localCount;
                            }
                            return localCount;
                        }).join();
                totalCount += count;
            } while (count >= limit);
            return CompletableFuture.completedFuture(Result.success(totalCount));
        }).join().getValue();

        if (totalRows == 0)
            return null;
        
        // Decompression has been deliberately moved out of transaction
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] compr : data) {
            decompress(compr, baos);
        }
        return baos.toByteArray();
    }

    /**
     * Save the file.
     * Overwrites the existing files.
     * For existing files, path specified is ignored (only id is used).
     * For new files, the folders are created as needed according to path value.
     * If the current version is frozen by snapshot, new version is created,
     * otherwise the current version is replaced.
     *
     * @param fid File id
     * @param path File path, for new files
     * @param author Author writing the file
     * @param data File data
     * @return true, if new file was created, false if the existing file was overwritten
     */
    public boolean saveFile(String fid, String path, String author, byte[] data) {
        // Compression moved out of transaction
        final List<byte[]> compressed = blockCompress(data);
        return retryContext.supplyResult(session -> {
            final Transaction trans = session
                    .beginTransaction(Transaction.Mode.SERIALIZABLE_READ_WRITE)
                    .join().getValue();
            final TxControl<?> tx = TxControl.id(trans).setCommitTx(false);
            final VersionDao vc = new VersionDao(session, tx, path, fid);
            vc.frozen = false;
            vc.author = author;
            vc.message = "-";
            // Check if file exists and whether its current version is frozen.
            File file = locateFile(session, tx, fid);
            if (file == null || file.isNull()) {
                vc.createFile(compressed);
            } else if (file.frozen) {
                vc.createVersion(compressed);
            } else {
                vc.vid = file.version;
                vc.replaceVersion(compressed);
            }
            trans.commit().join().expectSuccess();
            return CompletableFuture.completedFuture(Result.success(file==null));
        }).join().getValue();
    }

    public File locateFile(String fid) {
        File f = retryContext.supplyResult(session -> {
            final TxControl<?> tx = TxControl.onlineRo();
            return CompletableFuture.completedFuture(Result.success(
                    locateFile(session, tx, fid) ));
        }).join().getValue();
        if (f.isNull())
            return null;
        return f;
    }

    private File locateFile(Session session, TxControl<?> tx, String fid) {
        Params params = Params.of("$fid", PrimitiveValue.newText(fid));
        ResultSetReader rsr = session.executeDataQuery(query.checkFile, tx, params)
                .thenApply(Result::getValue)
                .thenApply(result -> result.getResultSet(0)).join();
        if (! rsr.next() )
            return NULL_FILE;
        File file = new File(fid,
                rsr.getColumn(0).getText(),
                rsr.getColumn(1).getText(),
                rsr.getColumn(2).getText());
        file.frozen = rsr.getColumn(3).getBool();
        return file;
    }

    public File locateFileByPath(String path) {
        final Path pFile = new Path(path);
        final Path pDir = new Path(pFile, 1);
        File f = retryContext.supplyResult(session -> {
            final TxControl<?> tx = TxControl.onlineRo();
            Folder dir = locateFolder(session, tx, pDir);
            if (dir==null)
                return CompletableFuture.completedFuture(Result.success(NULL_FILE));
            File tmp = locateFile(session, tx, dir.id, pFile.tail());
            if (tmp==null)
                tmp = NULL_FILE;
            return CompletableFuture.completedFuture(Result.success(tmp));
        }).join().getValue();
        if (f.isNull())
            return null;
        return f;
    }

    private File locateFile(Session session, TxControl<?> tx, String fparent, String fname) {
        Params params = Params.of("$fname", PrimitiveValue.newText(fname),
                "$fparent", PrimitiveValue.newText(fparent));
        ResultSetReader rsr = session.executeDataQuery(query.findFile, tx, params)
                .thenApply(Result::getValue)
                .thenApply(result -> result.getResultSet(0)).join();
        if (! rsr.next() )
            return null;
        File file = new File(rsr.getColumn(0).getText(),
                fparent,
                fname,
                rsr.getColumn(1).getText());
        file.frozen = rsr.getColumn(2).getBool();
        return file;
    }

    /**
     * Rename and move the existing file with the specified id to the new location.
     * The destination folders are created as necessary.
     *
     * @param fid File id
     * @param oldPath Current file path
     * @param newPath New file path
     */
    public void moveFile(String fid, String oldPath, String newPath) {
        final Path pNewFile = new Path(newPath);
        final Path pNewDir = new Path(pNewFile, 1);

        retryContext.supplyResult(session -> {
            final Transaction trans = session.
                    beginTransaction(Transaction.Mode.SERIALIZABLE_READ_WRITE)
                    .join().getValue();
            final TxControl<?> tx = TxControl.id(trans).setCommitTx(false);
            // Obtain the file id
            File file = locateFile(session, tx, fid);
            if (file==null)
                throw new RuntimeException("File not found: " + oldPath);
            // Create the destination folder
            Folder parent = createFolder(session, tx, pNewDir);
            // Move the file to the new destination folder
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$fparent", PrimitiveValue.newText(parent.id),
                    "$fname", PrimitiveValue.newText(pNewFile.tail()));
            session.executeDataQuery(query.moveFile, tx, params)
                    .join().getValue();
            trans.commit().join().expectSuccess();
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    /**
     * Rename and move the existing folder to the specified location.
     * The destination folders are created as necessary.
     * All the subobjects of the moved folder will have the new paths.
     *
     * @param oldFolderPath Current folder path
     * @param newFolderPath Desired new folder path
     */
    public void moveFolder(String oldFolderPath, String newFolderPath) {
        final Path pOldFolder = new Path(oldFolderPath);
        final Path pNewFolder = new Path(newFolderPath);
        final Path pNewContainer = new Path(pNewFolder, 1);

        retryContext.supplyResult(session -> {
            final Transaction trans = session.
                    beginTransaction(Transaction.Mode.SERIALIZABLE_READ_WRITE)
                    .join().getValue();
            final TxControl<?> tx = TxControl.id(trans).setCommitTx(false);
            // Locate the folder id
            Folder dirSrc = locateFolder(session, tx, pOldFolder);
            if (dirSrc==null) {
                throw new RuntimeException("Path not found: " + oldFolderPath);
            }
            // Create the destination folder
            Folder dirDst = createFolder(session, tx, pNewContainer);
            // Move the folder to its destination
            Params params = Params.of(
                    "$did", PrimitiveValue.newText(dirSrc.id),
                    "$dparent_old", PrimitiveValue.newText(dirSrc.parent),
                    "$dparent_new", PrimitiveValue.newText(dirDst.id),
                    "$dname_old", PrimitiveValue.newText(dirSrc.name),
                    "$dname_new", PrimitiveValue.newText(pNewFolder.tail())
            );
            session.executeDataQuery(query.moveFolder, tx, params)
                    .join().getValue();
            trans.commit().join().expectSuccess();
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    private Folder locateFolder(Session session, TxControl<?> tx, Path path) {
        ResultSetReader rsr;
        Folder parent = Folder.ROOT;
        for (String dname : path.entries) {
            Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                    "$dparent", PrimitiveValue.newText(parent.id));
            rsr = session.executeDataQuery(query.findFolder, tx, params)
                    .thenApply(Result::getValue)
                    .thenApply(result -> result.getResultSet(0)).join();
            if (! rsr.next()) {
                return null;
            }
            parent = new Folder(rsr.getColumn(0).getText(), parent.id, dname);
        }
        return parent;
    }

    private Folder createFolder(Session session, TxControl<?> tx, Path path) {
        ResultSetReader rsr;
        Folder parent = Folder.ROOT;
        boolean exists = true;
        for (String dname : path.entries) {
            if (exists) {
                Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(parent.id));
                rsr = session.executeDataQuery(query.findFolder, tx, params)
                        .thenApply(Result::getValue)
                        .thenApply(result -> result.getResultSet(0)).join();
                if (rsr.next()) {
                    parent = new Folder(rsr.getColumn(0).getText(), parent.id, dname);
                } else {
                    exists = false;
                }
            }
            if (!exists) {
                String did = UUID.randomUUID().toString();
                Params params = Params.of(
                        "$did", PrimitiveValue.newText(did),
                        "$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(parent.id));
                session.executeDataQuery(query.upsertFolder, tx, params).join().getValue();
                parent = new Folder(did, parent.id, dname);
            }
        }
        return parent;
    }

    /**
     * Remove the specified file.
     *
     * @param fid The file id to be removed.
     * @param path Path to the file to be removed.
     * @return true, if the file was actually deleted, false otherwise (only when path==null)
     */
    public boolean removeFile(String fid, String path) {
        return retryContext.supplyResult(session -> {
            File file = locateFile(session, TxControl.serializableRw().setCommitTx(false), fid);
            if (file==null) {
                if (path!=null) {
                    throw new RuntimeException("Path not found: " + path);
                }
                return CompletableFuture.completedFuture(Result.success(Boolean.FALSE));
            }
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid));
            session.executeDataQuery(query.deleteFile, 
                    TxControl.serializableRw().setCommitTx(true), params).join().getValue();
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    /**
     * Remove the folder and all its subobjects.
     *
     * @param folderPath The path to folder which needs to be removed.
     */
    public void removeFolder(String folderPath) {
        final Path path = new Path(folderPath);
        // Collect files and subfolders.
        final Set<String> files = new HashSet<>();
        final List<Folder> folders = new ArrayList<>();
        retryContext.supplyResult(session -> {
            final TxControl<?> tx = TxControl.onlineRo();
            Folder dir = locateFolder(session, tx, path);
            if (dir==null) {
                throw new RuntimeException("Path not found: " + folderPath);
            }
            final Stack<Folder> work = new Stack<>();
            work.push(dir);
            while (! work.empty()) {
                Folder curDir = work.pop();
                folders.add(curDir);
                files.addAll(listFiles(session, tx, curDir.id));
                for (Folder subDir : findSubFolders(session, tx, curDir.id)) {
                    work.push(subDir);
                }
            }
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
        // Dropping files one by one, file per transaction (slow! but safer)
        for (String fid : files) {
            removeFile(fid, null);
        }
        // Batch drop folders.
        Value<?>[] folderIds = folders.stream()
                .filter(value -> !(value==null || value.id.length()==0 || "/".equals(value.id)))
                .map(v -> (Value<?>)(StructValue.of(
                        "dparent", PrimitiveValue.newText(v.parent),
                        "dname", PrimitiveValue.newText(v.name))))
                .collect(Collectors.toCollection(ArrayList::new))
                .toArray(new Value<?>[0]);
        if (folderIds.length > 0) {
            Params params = Params.of("$values", ListValue.of(folderIds));
            retryContext.supplyResult(session -> {
                session.executeDataQuery(query.deleteFolders,
                        TxControl.serializableRw().setCommitTx(true), params).join().getValue();
                return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
            }).join().getValue();
        }
    }

    private List<Folder> findSubFolders(Session session, TxControl<?> tx, String dparent) {
        final List<Folder> retval = new ArrayList<>();
        Params params = Params.of(
                "$dparent", PrimitiveValue.newText(dparent));
        session.executeDataQuery(query.listFolders, tx, params)
                .thenApply(Result::getValue)
                .thenApply(result -> {
                    ResultSetReader rsr = result.getResultSet(0);
                    while (rsr.next()) {
                        String folderId = rsr.getColumn(0).getText();
                        if (! "/".equals(folderId)) {
                            retval.add(new Folder(folderId,
                                    dparent, rsr.getColumn(1).getText()));
                        }
                    }
                    return true;
                }).join();
        return retval;
    }

    private List<String> listFiles(Session session, TxControl<?> tx, String did) {
        final List<String> retval = new ArrayList<>();
        Params params = Params.of(
                "$did", PrimitiveValue.newText(did));
        session.executeDataQuery(query.listFiles, tx, params)
                .thenApply(Result::getValue)
                .thenApply(result -> {
                    ResultSetReader rsr = result.getResultSet(0);
                    while (rsr.next()) {
                        retval.add(rsr.getColumn(0).getText());
                    }
                    return true;
                }).join();
        return retval;
    }

    private static AuthProvider makeStaticAuthProvider(String authData) {
        int pos = authData.indexOf(":");
        if (pos < 0)
            return new StaticCredentials(authData, "");
        return new StaticCredentials(authData.substring(0, pos), authData.substring(pos+1));
    }

    private static List<byte[]> blockCompress(byte[] data) {
        final List<byte[]> compressed = new ArrayList<>();
        final int maxportion = 4 * 65536;
        int offset = 0;
        while (offset < data.length) {
            final int remainder = data.length - offset;
            int portion = maxportion;
            if (portion > remainder)
                portion = remainder;
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            compress(data, offset, portion, baos);
            compressed.add(baos.toByteArray());
            offset += portion;
        }
        return compressed;
    }

    private static void compress(byte[] input, int offset, int length, ByteArrayOutputStream output) {
        if (input==null || input.length==0)
            return;
        final Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);
        deflater.setInput(input, offset, length);
        deflater.finish();
        final byte[] temp = new byte[4096];
        while (! deflater.finished()) {
            int count = deflater.deflate(temp);
            if (count > 0)
                output.write(temp, 0, count);
        }
        deflater.end();
    }

    private static void decompress(byte[] input, ByteArrayOutputStream output) {
        if (input==null || input.length==0)
            return;
        final Inflater inflater = new Inflater();
        inflater.setInput(input);
        final byte[] temp = new byte[4096];
        try {
            while (! inflater.finished()) {
                int count = inflater.inflate(temp);
                if (count > 0)
                    output.write(temp, 0, count);
            }
        } catch(DataFormatException dfe) {
            throw new RuntimeException("Decompression failed", dfe);
        }
    }

    private class VersionDao {
        final Session session;
        final TxControl<?> tx;
        final String path;
        final String fid;
        String vid;
        boolean frozen = false;
        String author;
        String message;

        private VersionDao(Session session, TxControl<?> tx, String path, String fid) {
            this.session = session;
            this.tx = tx;
            this.path = path;
            this.fid = fid;
        }

        private void createFile(List<byte[]> data) {
            final Path myPath = new Path(this.path);
            final Path dirPath = new Path(myPath, 1);
            Folder dir = createFolder(session, tx, dirPath);
            String myVid = createVersion(data);
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$fparent", PrimitiveValue.newText(dir.id),
                    "$fname", PrimitiveValue.newText(myPath.tail()),
                    "$vid", PrimitiveValue.newText(myVid)
            );
            session.executeDataQuery(query.upsertFile, tx, params).join().getValue();
        }

        private String createVersion(List<byte[]> data) {
            vid = UUID.randomUUID().toString();
            frozen = false;
            upsertVersion(data);
            return vid;
        }

        private void replaceVersion(List<byte[]> data) {
            cleanupVersion();
            upsertVersion(data);
        }

        private void cleanupVersion() {
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$vid", PrimitiveValue.newText(vid)
            );
            session.executeDataQuery(query.cleanupVersion, tx, params).join().getValue();
        }

        private void upsertVersion(List<byte[]> data) {
            final String bid = UUID.randomUUID().toString();
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$vid", PrimitiveValue.newText(vid),
                    "$bid", PrimitiveValue.newText(bid),
                    "$frozen", PrimitiveValue.newBool(frozen),
                    "$tv", PrimitiveValue.newTimestamp(Instant.now()),
                    "$author", PrimitiveValue.newText(author),
                    "$message", PrimitiveValue.newText(message)
            );
            session.executeDataQuery(query.upsertVersion, tx, params).join().getValue();
            int pos = 0;
            for (byte[] b : data) {
                params = Params.of(
                        "$bid", PrimitiveValue.newText(bid),
                        "$pos", PrimitiveValue.newInt32(pos),
                        "$val", PrimitiveValue.newBytes(b)
                );
                // TODO: batch upsert
                session.executeDataQuery(query.upsertBytes, tx, params).join().getValue();
                ++pos;
            }
        }
    }

    public static String packageName(Class<?> clazz) {
        String fullName = clazz.getName();
        String shortName = clazz.getSimpleName();
        return fullName.substring(0, fullName.length() - (shortName.length() + 1));
    }

    private static class Queries {

        final String readFile;
        final String checkFile;
        final String findFolder;
        final String findFile;
        final String upsertFile;
        final String cleanupVersion;
        final String upsertVersion;
        final String upsertBytes;
        final String upsertFolder;
        final String moveFile;
        final String moveFolder;
        final String deleteFile;
        final String listFolders;
        final String listFiles;
        final String deleteFolders;

        Queries(String baseDir) {
            this.readFile = text("read-file", baseDir);
            this.checkFile = text("check-file", baseDir);
            this.findFolder = text("find-folder", baseDir);
            this.findFile = text("find-file", baseDir);
            this.upsertFile = text("upsert-file", baseDir);
            this.cleanupVersion = text("cleanup-version", baseDir);
            this.upsertVersion = text("upsert-version", baseDir);
            this.upsertBytes = text("upsert-bytes", baseDir);
            this.upsertFolder = text("upsert-folder", baseDir);
            this.moveFile = text("move-file", baseDir);
            this.moveFolder = text("move-folder", baseDir);
            this.deleteFile = text("delete-file", baseDir);
            this.deleteFolders = text("delete-folders", baseDir);
            this.listFolders = text("list-folders", baseDir);
            this.listFiles = text("list-files", baseDir);
        }

        private static final String DIR = packageName(YdbFs.class).replace('.', '/') + "/qtext/";

        public static String text(String id, String baseDir) {
            final StringBuilder sb = new StringBuilder();
            sb.append("--!syntax_v1\n");
            sb.append("PRAGMA TablePathPrefix(\"").append(baseDir).append("\");\n");
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try (InputStream stream = loader.getResourceAsStream(DIR + id + ".sql")) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stream, StandardCharsets.UTF_8));
                char[] buffer = new char[100];
                int nchars;
                while ((nchars=reader.read(buffer)) != -1) {
                    sb.append(buffer, 0, nchars);
                }
            } catch(IOException ix) {
                throw new IllegalStateException("Failed to load query text " + id, ix);
            }
            return sb.toString();
        }
    }

    /**
     * Authentication mode
     */
    public static enum AuthMode {
        /**
         * Cloud compute instance metadata
         */
        METADATA,
        /**
         * Service account key file, path to the file
         */
        SAKEY,
        /**
         * Static credentials, username:password
         */
        STATIC,
        /**
         * Anonymous
         */
        NONE,
    }

    public static class Path {
        public final String[] entries;

        public Path(String[] entries) {
            this.entries = entries;
        }

        public Path(Path p, int skip) {
            skip = (skip > 0) ? skip : 0;
            int sz = (p.entries.length > skip) ? (p.entries.length - skip) : 0;
            this.entries = new String[sz];
            System.arraycopy(p.entries, 0, this.entries, 0, sz);
        }

        public Path(Path p) {
            this(p, 0);
        }

        public Path(String path, int skip) {
            List<String> tmp = Arrays.asList(path.split("/")).stream()
                    .filter(v -> !(v==null || v.length()==0))
                    .collect(Collectors.toList());
            while (skip > 0 && !tmp.isEmpty()) {
                tmp.remove(tmp.size()-1);
                --skip;
            }
            this.entries = tmp.toArray(new String[0]);
        }

        public Path(String path) {
            this(path, 0);
        }

        // reverse-list constructor
        public Path(List<String> elements) {
            final int n = elements.size();
            this.entries = new String[n];
            for (int i=0; i<n; ++i) {
                this.entries[i] = elements.get(n-(i+1));
            }
        }

        public String tail() {
            if (entries.length==0)
                return "/";
            return entries[entries.length - 1];
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            boolean slash = false;
            for (String v : entries) {
                if (slash)
                    sb.append("/");
                else
                    slash = true;
                sb.append(v);
            }
            return sb.toString();
        }
    }

    private static final File NULL_FILE = new File(null, null, null, null);
    
    public static class File implements Serializable {
        public final String id;
        public final String parent;
        public final String name;
        public final String version;
        public boolean frozen;

        public File(String id, String parent, String name, String version) {
            this.id = id;
            this.parent = parent;
            this.name = name;
            this.version = version;
            this.frozen = false;
        }

        public boolean isNull() {
            return (id==null);
        }
    }

    public static class Folder implements Serializable {
        public final String id;
        public final String parent;
        public final String name;
        public final List<Folder> children = new ArrayList<>();
        public final List<File> files = new ArrayList<>();

        public static final Folder ROOT = new Folder("/", "/", "/");

        public Folder(String id, String parent, String name) {
            this.id = id;
            this.parent = parent;
            this.name = name;
        }

        public boolean isRoot() {
            return (parent==null) || id.equals(parent);
        }

        public boolean isLeaf() {
            return (parent!=null) && !id.equals(parent);
        }
    }

    public static class FullList implements Serializable {
        public final Map<String, Folder> folders;
        public final Map<String, File> files;
        public final Folder root;

        public FullList(Map<String, Folder> folders, Map<String, File> files) {
            this.folders = folders;
            this.files = files;
            this.root = folders.get("/");
        }

        private FullList fill() {
            for (Folder f : folders.values()) {
                if ( f.isLeaf() ) {
                    Folder p = folders.get(f.parent);
                    if (p!=null) {
                        p.children.add(f);
                    }
                }
            }
            for (File f : files.values()) {
                Folder p = folders.get(f.parent);
                if (p!=null) {
                    p.files.add(f);
                }
            }
            return this;
        }

        public Path buildPath(File file) {
            final List<String> elements = new ArrayList<>();
            elements.add(file.name);
            Folder folder = folders.get(file.parent);
            while (folder != null && folder.isLeaf() ) {
                elements.add(folder.name);
                folder = folders.get(folder.parent);
            }
            return new Path(elements);
        }
    }

}
