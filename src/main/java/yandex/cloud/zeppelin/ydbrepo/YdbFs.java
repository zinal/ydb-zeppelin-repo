package yandex.cloud.zeppelin.ydbrepo;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
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
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ReadTableSettings;
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

    private static final CompletableFuture<Result<Boolean>> ASYNC_TRUE =
            CompletableFuture.completedFuture(Result.success(Boolean.TRUE));

    private static final CompletableFuture<Result<Boolean>> ASYNC_FALSE =
            CompletableFuture.completedFuture(Result.success(Boolean.FALSE));

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
                gtb.withAuthProvider(makeStaticCredentials(authData));
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
            .orderedRead(false)
            .columns("did", "dparent", "dname").build();
        try (Session session = tableClient.createSession(Duration.ofSeconds(10)).join().getValue()) {
            session.readTable(basePath + "/zdir", settings).start((ResultSetReader rs) -> {
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
            }).join().expectSuccess();
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
            .orderedRead(false)
            .columns("fid", "fparent", "fname", "vid").build();
        try (Session session = tableClient.createSession(Duration.ofSeconds(10)).join().getValue()) {
            session.readTable(basePath + "/zfile", settings).start((ResultSetReader rs) -> {
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
            }).join().expectSuccess();
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
     * @param fid File id
     * @param vid File revision id, or null for the current revision
     * @return File data, or null if the file or revision does not exist.
     */
    public byte[] readFile(String fid, String vid) {
        final List<byte[]> data = new ArrayList<>();

        boolean hasFile = retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            String bid;
            if (vid==null || vid.length()==0) {
                bid = lookupBlobCur(txh, fid);
            } else {
                bid = lookupBlobRev(txh, fid, vid);
            }
            if (bid==null)
                return ASYNC_FALSE;
            readFile(txh, bid, data);
            return ASYNC_TRUE;
        }).join().getValue();

        if (! hasFile)
            return null;

        // Decompression has been deliberately moved out of transaction
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] compr : data) {
            decompress(compr, baos);
        }
        return baos.toByteArray();
    }

    private String lookupBlobCur(TxHandler txh, String fid) {
        ResultSetReader rsr = txh.executeDataQuery(query.lookubBlobCur,
                Params.of("$fid", PrimitiveValue.newText(fid))).getResultSet(0);
        if (!rsr.next())
            return "";
        String bid = rsr.getColumn(0).getText();
        return (bid==null || bid.length()==0) ? null : bid;
    }

    private String lookupBlobRev(TxHandler txh, String fid, String vid) {
        ResultSetReader rsr = txh.executeDataQuery(query.lookubBlobRev,
                Params.of("$fid", PrimitiveValue.newText(fid),
                        "$vid", PrimitiveValue.newText(vid))).getResultSet(0);
        if (!rsr.next())
            return "";
        String bid = rsr.getColumn(0).getText();
        return (bid==null || bid.length()==0) ? null : bid;
    }

    private void readFile(TxHandler txh, String bid, List<byte[]> data) {
        final int limit = 10;
        final PrimitiveValue vlimit = PrimitiveValue.newInt32(limit);
        final PrimitiveValue vbid = PrimitiveValue.newText(bid);

        int pos = -1;
        int count;
        do {
            count = 0;
            Params params = Params.of("$bid", vbid, "$limit", vlimit,
                    "$pos", PrimitiveValue.newInt32(pos));
            ResultSetReader rsr = txh.executeDataQuery(query.readFile, params).getResultSet(0);
            while (rsr.next()) {
                data.add(rsr.getColumn(0).getBytes());
                int curPos = rsr.getColumn(1).getInt32();
                if (curPos > pos)
                    pos = curPos;
                ++count;
            }
        } while (count >= limit);
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
            String actualFid = fid;
            final TxHandler txh = new TxHandler(session, TxControl.serializableRw());
            // Check if file exists and whether its current version is frozen.
            final File file;
            if (fid==null) {
                file = locateFileByPath(txh, path);
                if (file!=null)
                    actualFid = file.id;
            } else {
                file = locateFile(txh, fid);
            }
            final VersionDao vc = new VersionDao(txh, path, actualFid);
            vc.frozen = false;
            vc.author = author;
            vc.message = "-";
            if (file == null || file.isNull()) {
                vc.createFile(compressed);
            } else if (file.frozen) {
                vc.createVersion(compressed);
            } else {
                vc.vid = file.version;
                vc.replaceVersion(compressed);
            }
            txh.commit();
            return (file==null) ? ASYNC_TRUE : ASYNC_FALSE;
        }).join().getValue();
    }

    /**
     * Locate file description by file id.
     *
     * @param fid File id
     * @return File description, or null if file id was not found.
     */
    public File locateFile(String fid) {
        File f = retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            return CompletableFuture.completedFuture(Result.success(
                    locateFile(txh, fid) ));
        }).join().getValue();
        if (f.isNull())
            return null;
        return f;
    }

    private File locateFile(TxHandler txh, String fid) {
        Params params = Params.of("$fid", PrimitiveValue.newText(fid));
        ResultSetReader rsr = txh.executeDataQuery(query.checkFile, params).getResultSet(0);
        if (! rsr.next() )
            return NULL_FILE;
        File file = new File(fid,
                rsr.getColumn(0).getText(),
                rsr.getColumn(1).getText(),
                rsr.getColumn(2).getText());
        file.frozen = rsr.getColumn(3).getBool();
        return file;
    }

    /**
     * Locate file description by file path.
     *
     * @param path File path
     * @return File description, or null if file path was not found.
     */
    public File locateFileByPath(String path) {
        File f = retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            File file = locateFileByPath(txh, path);
            if (file==null)
                file = NULL_FILE;
            return CompletableFuture.completedFuture(Result.success(file));
        }).join().getValue();
        if (f==null || f.isNull())
            return null;
        return f;
    }

    private File locateFileByPath(TxHandler txh, String path) {
        final Path pFile = new Path(path);
        final Path pDir = new Path(pFile, 1);

        Folder dir = locateFolder(txh, pDir);
        if (dir==null)
            return null;
        return locateFile(txh, dir.id, pFile.tail());
    }

    /**
     * Locate file description by directory id and file name.
     *
     * @param fparent Directory id
     * @param fname File name (the last part of file path)
     * @return File description, or null if directory id is unknown
     *         or file with that name was not found.
     */
    public File locateFile(String fparent, String fname) {
        File f = retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            File tmp = locateFile(txh, fparent, fname);
            if (tmp==null)
                tmp = NULL_FILE;
            return CompletableFuture.completedFuture(Result.success(tmp));
        }).join().getValue();
        if (f.isNull())
            return null;
        return f;
    }

    private File locateFile(TxHandler txh, String fparent, String fname) {
        Params params = Params.of("$fname", PrimitiveValue.newText(fname),
                "$fparent", PrimitiveValue.newText(fparent));
        ResultSetReader rsr = txh.executeDataQuery(query.findFile, params).getResultSet(0);
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
            final TxHandler txh = new TxHandler(session, TxControl.serializableRw());
            // Obtain the file id
            final File file;
            if (fid==null) {
                file = locateFileByPath(txh, oldPath);
            } else {
                file = locateFile(txh, fid);
            }
            if (file==null)
                throw new RuntimeException("File not found: " + oldPath);
            // Create the destination folder
            Folder parent = createFolder(txh, pNewDir);
            // Move the file to the new destination folder
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(file.id),
                    "$fparent", PrimitiveValue.newText(parent.id),
                    "$fparent_old", PrimitiveValue.newText(file.parent),
                    "$fname", PrimitiveValue.newText(pNewFile.tail()),
                    "$fname_old", PrimitiveValue.newText(file.name));
            txh.executeDataQuery(query.moveFile, params);
            txh.commit();
            return ASYNC_TRUE;
        }).join().getValue();
    }

    /**
     * Rename and move the existing folder to the specified location.
     * The destination folders are created as necessary.
     * All the sub-objects of the moved folder will have the new paths.
     *
     * @param oldFolderPath Current folder path
     * @param newFolderPath Desired new folder path
     */
    public void moveFolder(String oldFolderPath, String newFolderPath) {
        final Path pOldFolder = new Path(oldFolderPath);
        final Path pNewFolder = new Path(newFolderPath);
        final Path pNewContainer = new Path(pNewFolder, 1);

        retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.serializableRw());
            // Locate the folder id
            Folder dirSrc = locateFolder(txh, pOldFolder);
            if (dirSrc==null) {
                throw new RuntimeException("Path not found: " + oldFolderPath);
            }
            // Create the destination folder
            Folder dirDst = createFolder(txh, pNewContainer);
            // Move the folder to its destination
            Params params = Params.of(
                    "$did", PrimitiveValue.newText(dirSrc.id),
                    "$dparent_old", PrimitiveValue.newText(dirSrc.parent),
                    "$dparent_new", PrimitiveValue.newText(dirDst.id),
                    "$dname_old", PrimitiveValue.newText(dirSrc.name),
                    "$dname_new", PrimitiveValue.newText(pNewFolder.tail())
            );
            txh.executeDataQuery(query.moveFolder, params);
            txh.commit();
            return ASYNC_TRUE;
        }).join().getValue();
    }

    /**
     * Find directory id by directory path.
     *
     * @param folderPath Directory path
     * @return Directory id, or null if path was not found.
     */
    public String locateFolder(String folderPath) {
        final Path pFolder = new Path(folderPath);
        String fid = retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            // Locate the folder id
            Folder dir = locateFolder(txh, pFolder);
            if (dir==null) {
                return CompletableFuture.completedFuture(Result.success(""));
            }
            return CompletableFuture.completedFuture(Result.success(dir.id));
        }).join().getValue();
        return (fid==null || fid.length()==0) ? null : fid;
    }

    private Folder locateFolder(TxHandler txh, Path path) {
        ResultSetReader rsr;
        Folder parent = Folder.ROOT;
        for (String dname : path.entries) {
            Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                    "$dparent", PrimitiveValue.newText(parent.id));
            rsr = txh.executeDataQuery(query.findFolder, params).getResultSet(0);
            if (! rsr.next()) {
                return null;
            }
            parent = new Folder(rsr.getColumn(0).getText(), parent.id, dname);
        }
        return parent;
    }

    private Folder createFolder(TxHandler txh, Path path) {
        ResultSetReader rsr;
        Folder parent = Folder.ROOT;
        boolean exists = true;
        for (String dname : path.entries) {
            if (exists) {
                Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(parent.id));
                rsr = txh.executeDataQuery(query.findFolder, params).getResultSet(0);
                if (rsr.next()) {
                    parent = new Folder(rsr.getColumn(0).getText(), parent.id, dname);
                } else {
                    exists = false;
                }
            }
            if (!exists) {
                String did = newId();
                Params params = Params.of(
                        "$did", PrimitiveValue.newText(did),
                        "$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(parent.id));
                txh.executeDataQuery(query.upsertFolder, params);
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
     */
    public void removeFile(String fid, String path) {
        retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.serializableRw());
            if (path != null) {
                // fid by path retrieval or fid validation
                File file;
                if (fid==null) {
                    file = locateFileByPath(txh, path);
                } else {
                    file = locateFile(txh, fid);
                }
                if (file==null) {
                    throw new RuntimeException("Path not found: " + path);
                }
            }
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid));
            txh.executeDataQuery(query.deleteFile, params);
            txh.commit();
            return ASYNC_TRUE;
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
            final TxHandler txh = new TxHandler(session, TxControl.snapshotRo());
            Folder dir = locateFolder(txh, path);
            if (dir==null) {
                throw new RuntimeException("Path not found: " + folderPath);
            }
            final Stack<Folder> work = new Stack<>();
            work.push(dir);
            while (! work.empty()) {
                Folder curDir = work.pop();
                folders.add(curDir);
                files.addAll(listFiles(txh, curDir.id));
                for (Folder subDir : findSubFolders(txh, curDir.id)) {
                    work.push(subDir);
                }
            }
            return ASYNC_TRUE;
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
                return ASYNC_TRUE;
            }).join().getValue();
        }
    }

    private List<Folder> findSubFolders(TxHandler txh, String dparent) {
        final List<Folder> retval = new ArrayList<>();
        Params params = Params.of(
                "$dparent", PrimitiveValue.newText(dparent));
        ResultSetReader rsr = txh.executeDataQuery(query.listFolders, params).getResultSet(0);
        while (rsr.next()) {
            String folderId = rsr.getColumn(0).getText();
            if (! "/".equals(folderId)) {
                retval.add(new Folder(folderId,
                        dparent, rsr.getColumn(1).getText()));
            }
        }
        return retval;
    }

    private List<String> listFiles(TxHandler txh, String did) {
        final List<String> retval = new ArrayList<>();
        Params params = Params.of(
                "$did", PrimitiveValue.newText(did));
        ResultSetReader rsr = txh.executeDataQuery(query.listFiles, params).getResultSet(0);
        while (rsr.next()) {
            retval.add(rsr.getColumn(0).getText());
        }
        return retval;
    }

    /**
     * Add the version checkpoint to the specified file.
     *
     * @param fid File id
     * @param path File path
     * @param message
     * @param author
     * @param stamp
     * @return
     */
    public String checkpoint(String fid, String path, 
            String message, String author, Instant stamp) {
        return retryContext.supplyResult(session -> {
            final TxHandler txh = new TxHandler(session, TxControl.serializableRw());
            File file;
            if (fid==null) {
                file = locateFileByPath(txh, path);
            } else {
                file = locateFile(txh, fid);
            }
            if (file==null) {
                throw new RuntimeException("Path not found: " + path);
            }
            final String versionId;
            if (file.frozen) {
                String bid = lookupBlobCur(txh, file.id);
                final VersionDao dao = new VersionDao(txh, "" /*unused*/, file.id);
                dao.frozen = true;
                dao.vid = null;
                dao.message = message;
                dao.author = author;
                versionId = dao.createVersion(bid, stamp);
            } else {
                Params params = Params.of(
                        "$fid", PrimitiveValue.newText(file.id),
                        "$vid", PrimitiveValue.newText(file.version),
                        "$frozen", PrimitiveValue.newBool(true),
                        "$tv", PrimitiveValue.newTimestamp(stamp),
                        "$author", PrimitiveValue.newText(author),
                        "$message", PrimitiveValue.newText(message)
                );
                txh.executeDataQuery(query.freezeVersion, params);
                versionId = file.version;
            }
            txh.commit();
            return CompletableFuture.completedFuture(Result.success(versionId));
        }).join().getValue();
    }

    public List<Revision> listHistory(String fid) {
        final List<Revision> retval = new ArrayList<>();
        try (Session session = tableClient.createSession(Duration.ofSeconds(10)).join().getValue()) {
            session.executeScanQuery(query.listRevisions,
                    Params.of("$fid", PrimitiveValue.newText(fid)),
                    ExecuteScanQuerySettings.newBuilder().build()).start((ResultSetReader rs) -> {
                final int vid = rs.getColumnIndex("vid");
                final int tv = rs.getColumnIndex("tv");
                final int author = rs.getColumnIndex("author");
                final int message = rs.getColumnIndex("message");
                while (rs.next()) {
                    retval.add(new Revision(
                            rs.getColumn(vid).getText(),
                            rs.getColumn(tv).getTimestamp(),
                            rs.getColumn(author).getText(),
                            rs.getColumn(message).getText()
                    ));
                }
            }).join().expectSuccess();
        }
        Collections.sort(retval, (o1, o2) -> cmplong(o1.tv, o2.tv));
        return retval;
    }

    private static int cmplong(long v1, long v2) {
        if (v1==v2)
            return 0;
        if (v1>v2)
            return 1;
        return -1;
    }

    private static StaticCredentials makeStaticCredentials(String authData) {
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
        final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
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

    public static String newId() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.putLong(uuid.getMostSignificantBits());
        return Base64.getUrlEncoder().encodeToString(buffer.array()).replace('=', ' ').trim();
    }

    private class VersionDao {
        final TxHandler txh;
        final String path;
        final String fid;
        String vid;
        boolean frozen = false;
        String author;
        String message;

        private VersionDao(TxHandler txh, String path, String fid) {
            this.txh = txh;
            this.path = path;
            this.fid = fid;
        }

        private void createFile(List<byte[]> data) {
            final Path myPath = new Path(this.path);
            final Path dirPath = new Path(myPath, 1);
            Folder dir = createFolder(txh, dirPath);
            String myVid = createVersion(data);
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$fparent", PrimitiveValue.newText(dir.id),
                    "$fname", PrimitiveValue.newText(myPath.tail()),
                    "$vid", PrimitiveValue.newText(myVid)
            );
            txh.executeDataQuery(query.upsertFile, params);
        }

        private String createVersion(List<byte[]> data) {
            vid = newId();
            frozen = false;
            upsertVersion(data);
            return vid;
        }

        private String createVersion(String bid, Instant stamp) {
            vid = newId();
            frozen = true;
            upsertVersion(bid, stamp);
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
            txh.executeDataQuery(query.cleanupVersion, params);
        }

        private void upsertVersion(String bid, Instant stamp) {
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$vid", PrimitiveValue.newText(vid),
                    "$bid", PrimitiveValue.newText(bid),
                    "$frozen", PrimitiveValue.newBool(frozen),
                    "$tv", PrimitiveValue.newTimestamp(stamp),
                    "$author", PrimitiveValue.newText(author),
                    "$message", PrimitiveValue.newText(message)
            );
            txh.executeDataQuery(query.upsertVersion, params);
        }

        private void upsertVersion(List<byte[]> data) {
            final String bid = newId();
            upsertVersion(bid, Instant.now());
            int pos = 0;
            for (byte[] b : data) {
                Params params = Params.of(
                        "$bid", PrimitiveValue.newText(bid),
                        "$pos", PrimitiveValue.newInt32(pos),
                        "$val", PrimitiveValue.newBytes(b)
                );
                // TODO: batch upsert
                txh.executeDataQuery(query.upsertBytes, params);
                ++pos;
            }
        }
    }

    public static String packageName(Class<?> clazz) {
        String fullName = clazz.getName();
        String shortName = clazz.getSimpleName();
        return fullName.substring(0, fullName.length() - (shortName.length() + 1));
    }

    public static class TxHandler {
        final Session session;
        TxControl<?> tx;
        String txId;
        static CommitTxSettings commitTxSettings = new CommitTxSettings();

        TxHandler(Session session, TxControl<?> tx) {
            this.session = session;
            this.tx = tx.setCommitTx(false);
            this.txId = null;
        }

        DataQueryResult executeDataQuery(String query, Params params) {
            DataQueryResult result = session.executeDataQuery(query, tx, params).join().getValue();
            if (txId==null) {
                txId = result.getTxId();
                tx = TxControl.id(txId).setCommitTx(false);
            }
            return result;
        }

        void commit() {
            if (txId != null) {
                session.commitTransaction(txId, commitTxSettings).join().expectSuccess();
            }
        }
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

        public boolean isEmpty() {
            return (entries.length == 0) ||
                    (entries.length == 1 && "/".equals(entries[0]));
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

    public static class Revision {
        public String rid;
        public long tv;
        public String author;
        public String message;

        public Revision() {}

        public Revision(String rid, long tv, String author, String message) {
            this.rid = rid;
            this.tv = tv;
            this.author = author;
            this.message = message;
        }

        public Revision(String rid, Instant tv, String author, String message) {
            this(rid, (tv==null) ? -1L : tv.getEpochSecond(), author, message);
        }
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
        final String freezeVersion;
        final String lookubBlobCur;
        final String lookubBlobRev;
        final String listRevisions;

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
            this.freezeVersion = text("freeze-version", baseDir);
            this.lookubBlobCur = text("lookup-blob-cur", baseDir);
            this.lookubBlobRev = text("lookup-blob-rev", baseDir);
            this.listRevisions = text("list-revisions", baseDir);
        }

        private static final String DIR = packageName(YdbFs.class).replace('.', '/') + "/qtext/";

        public static String text(String id, String baseDir) {
            final StringBuilder sb = new StringBuilder();
            sb.append("--!syntax_v1\n");
            sb.append("PRAGMA TablePathPrefix(\"").append(baseDir).append("\");\n");
            ClassLoader loader = YdbFs.class.getClassLoader();
            try (InputStream stream = loader.getResourceAsStream(DIR + id + ".sql")) {
                if (stream==null)
                    throw new IOException("Resource not found");
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

}
