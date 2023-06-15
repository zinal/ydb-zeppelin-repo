package com.yandex.cloud.zeppelin.ydbrepo;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

/**
 * Basic CRUD operations for files and folders stored in YDB.
 * 
 * @author mzinal
 */
public class YdbFs implements AutoCloseable {

    private final String baseDir;
    private final String database;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryContext;

    private final String queryReadFile;
    private final String queryCheckFile;
    private final String queryFindFolder;
    private final String queryUpsertFile;
    private final String queryCleanupVersion;
    private final String queryUpsertVersion;
    private final String queryUpsertBytes;
    private final String queryUpsertFolder;
    private final String queryMoveFile;
    private final String queryMoveFolder;
    private final String queryDeleteFile;

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
        this.baseDir = baseDir;
        this.queryReadFile = buildReadFile(baseDir);
        this.queryCheckFile = buildCheckFile(baseDir);
        this.queryFindFolder = buildFindFolder(baseDir);
        this.queryUpsertFile = buildUpsertFile(baseDir);
        this.queryCleanupVersion = buildCleanupVersion(baseDir);
        this.queryUpsertVersion = buildUpsertVersion(baseDir);
        this.queryUpsertBytes = buildUpsertBytes(baseDir);
        this.queryUpsertFolder = buildUpsertFolder(baseDir);
        this.queryMoveFile = buildMoveFile(baseDir);
        this.queryMoveFolder = buildMoveFolder(baseDir);
        this.queryDeleteFile = buildDeleteFile(baseDir);
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
            session.readTable(database + "/" + baseDir + "zdir", settings, rs -> {
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
            session.readTable(database + "/" + baseDir + "zfile", settings, rs -> {
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
        final TxControl tx = TxControl.onlineRo();
        final int limit = 10;
        final PrimitiveValue vlimit = PrimitiveValue.newInt32(limit);

        int totalRows = retryContext.supplyResult(session -> {
            final int[] pos = new int[] { -1 };
            int count, totalCount = 0;
            do {
                Params params = Params.of("$fid", vid, "$limit", vlimit,
                        "$pos", PrimitiveValue.newInt32(pos[0]));
                count = session.executeDataQuery(queryReadFile, tx, params)
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

    private static String buildReadFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "DECLARE $pos AS Int32;\n"
                + "DECLARE $limit AS Int32;\n"
                + "SELECT b.val, b.pos "
                + "FROM (SELECT vid FROM zfile WHERE fid=$fid) AS f "
                + "INNER JOIN zbytes b ON b.vid=f.vid "
                + "WHERE b.pos > $pos "
                + "ORDER BY b.pos LIMIT $limit", baseDir);
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
        final TxControl tx = TxControl.serializableRw();
        return retryContext.supplyResult(session -> {
            final VersionDao vc = new VersionDao(session, tx, path, fid);
            vc.frozen = false;
            vc.author = author;
            vc.message = "-";
            // Check if file exists and whether its current version is frozen.
            File file = locateFile(session, tx, fid);
            if (file == null) {
                vc.createFile(compressed);
            } else if (file.frozen) {
                vc.createVersion(compressed);
            } else {
                vc.vid = file.version;
                vc.replaceVersion(compressed);
            }
            return CompletableFuture.completedFuture(Result.success(file==null));
        }).join().getValue();
    }

    private File locateFile(Session session, TxControl tx, String fid) {
        Params params = Params.of("$fid", PrimitiveValue.newText(fid));
        ResultSetReader rsr = session.executeDataQuery(queryCheckFile, tx, params)
                .thenApply(Result::getValue)
                .thenApply(result -> result.getResultSet(0)).join();
        if (! rsr.next() )
            return null;
        File file = new File(fid,
                rsr.getColumn(0).getText(),
                rsr.getColumn(1).getText(),
                rsr.getColumn(2).getText());
        file.frozen = rsr.getColumn(3).getBool();
        return file;
    }

    private static String buildCheckFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "SELECT f.fparent,f.fname,v.vid,v.frozen"
                + "FROM (SELECT vid,fparent,fname FROM zfile WHERE fid=$fid) AS f "
                + "INNER JOIN zver v ON v.vid=f.vid", baseDir);
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

        final TxControl tx = TxControl.serializableRw();
        retryContext.supplyResult(session -> {
            File file = locateFile(session, tx, fid);
            if (file==null)
                throw new RuntimeException("File not found: " + oldPath);
            String did = createFolder(session, tx, pNewDir);
            moveFile(session, tx, fid, did, pNewFile.tail());
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    private void moveFile(Session session, TxControl tx, String fid, String fparent, String fname) {
        Params params = Params.of(
                "$fid", PrimitiveValue.newText(fid),
                "$fparent", PrimitiveValue.newText(fparent),
                "$fname", PrimitiveValue.newText(fname));
        session.executeDataQuery(queryMoveFile, tx, params).join().getValue();
    }

    private static String buildMoveFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "DECLARE $fparent AS Utf8;\n"
                + "DECLARE $fname AS Utf8;\n"
                + "UPSERT INTO zfile (fid, fparent, fname) "
                + "VALUES ($fid, $fparent, $fname)", baseDir);
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

        final TxControl tx = TxControl.serializableRw();
        retryContext.supplyResult(session -> {
            String srcId = locateFolder(session, tx, pOldFolder);
            if (srcId==null) {
                throw new RuntimeException("Path not found: " + oldFolderPath);
            }
            String dstId = createFolder(session, tx, pNewContainer);
            moveFolder(session, tx, srcId, dstId, pNewFolder.tail());
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    private void moveFolder(Session session, TxControl tx, String srcId, String dstId, String name) {
        Params params = Params.of(
                "$srcId", PrimitiveValue.newText(srcId),
                "$dstId", PrimitiveValue.newText(dstId),
                "$name", PrimitiveValue.newText(name));
        session.executeDataQuery(queryMoveFolder, tx, params).join().getValue();
    }

    private static String buildMoveFolder(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $srcId AS Utf8;\n"
                + "DECLARE $dstId AS Utf8;\n"
                + "DECLARE $name AS Utf8;\n"
                + "UPSERT INTO zdir (did, dparent, dname) "
                + "VALUES ($srcId, $dstId, $name)", baseDir);
    }

    private String locateFolder(Session session, TxControl tx, Path path) {
        ResultSetReader rsr;
        String dparent = "/";
        for (String dname : path.entries) {
            Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                    "$dparent", PrimitiveValue.newText(dparent));
            rsr = session.executeDataQuery(queryFindFolder, tx, params)
                    .thenApply(Result::getValue)
                    .thenApply(result -> result.getResultSet(0)).join();
            if (! rsr.next()) {
                return null;
            }
            dparent = rsr.getColumn(0).getText();
        }
        return dparent;
    }

    private String createFolder(Session session, TxControl tx, Path path) {
        ResultSetReader rsr;
        String dparent = "/";
        boolean exists = true;
        for (String dname : path.entries) {
            if (exists) {
                Params params = Params.of("$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(dparent));
                rsr = session.executeDataQuery(queryFindFolder, tx, params)
                        .thenApply(Result::getValue)
                        .thenApply(result -> result.getResultSet(0)).join();
                if (rsr.next()) {
                    dparent = rsr.getColumn(0).getText();
                } else {
                    exists = false;
                }
            }
            if (!exists) {
                String did = UUID.randomUUID().toString();
                Params params = Params.of(
                        "$did", PrimitiveValue.newText(did),
                        "$dname", PrimitiveValue.newText(dname),
                        "$dparent", PrimitiveValue.newText(dparent));
                session.executeDataQuery(queryUpsertFolder, tx, params).join().getValue();
            }
        }
        return dparent;
    }

    private static String buildFindFolder(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $dname AS Utf8;\n"
                + "DECLARE $dparent AS Utf8;\n"
                + "SELECT did FROM zdir VIEW naming "
                + "WHERE dparent=$dparent AND dname=$dname", baseDir);
    }

    private static String buildUpsertFolder(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $did AS Utf8;\n"
                + "DECLARE $dname AS Utf8;\n"
                + "DECLARE $dparent AS Utf8;\n"
                + "UPSERT INTO zdir(did,dname,dparent) "
                + "VALUES($did,$dname,$dparent);", baseDir);
    }

    /**
     * Remove the specified file.
     *
     * @param fid The file id to be removed.
     * @param path Path to the file to be removed.
     */
    public void removeFile(String fid, String path) {
        final TxControl tx = TxControl.serializableRw();
        retryContext.supplyResult(session -> {
            File file = locateFile(session, tx, fid);
            if (file==null) {
                throw new RuntimeException("Path not found: " + path);
            }
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid));
            session.executeDataQuery(queryDeleteFile, tx, params).join().getValue();
            return CompletableFuture.completedFuture(Result.success(Boolean.TRUE));
        }).join().getValue();
    }

    private static String buildDeleteFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "$qfile=(SELECT fid FROM zfile WHERE fid=$fid);\n"
                + "$qver=(SELECT fid, vid FROM zver WHERE fid=$fid);\n"
                + "$qbytes=(\n"
                + "  SELECT b.vid, b.pos \n"
                + "  FROM $qver v \n"
                + "  INNER JOIN zbytes b ON b.vid=v.vid);\n"
                + "DISCARD SELECT * FROM $qfile;\n"
                + "DISCARD SELECT * FROM $qbytes;\n"
                + "DELETE FROM zbytes ON SELECT * FROM $qbytes;\n"
                + "DELETE FROM zver ON SELECT * FROM $qver;"
                + "DELETE FROM zfile ON SELECT * FROM $qfile;", baseDir);
    }

    /**
     * Remove the folder and all its subobjects.
     *
     * @param folderPath The path to folder which needs to be removed.
     */
    public void removeFolder(String folderPath) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
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
            if (portion < remainder)
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

    private static String buildCleanupVersion(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $vid AS Utf8;\n"
                + "DELETE FROM zbytes WHERE vid=$vid;\n"
                + "DELETE FROM zver WHERE vid=$vid;", baseDir);
    }

    private static String buildUpsertVersion(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $vid AS Utf8;\n"
                + "DECLARE $fid AS Utf8;\n"
                + "DECLARE $frozen AS Bool;\n"
                + "DECLARE $tv AS Timestamp;\n"
                + "DECLARE $author AS Utf8;\n"
                + "DECLARE $message AS Utf8;\n"
                + "UPSERT INTO zver(vid,fid,frozen,tv,author,message) "
                + "VALUES($vid,$fid,$frozen,$tv,$author,$message);", baseDir);
    }

    private static String buildUpsertBytes(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $vid AS Utf8;\n"
                + "DECLARE $pos AS Int32;\n"
                + "DECLARE $val AS String;\n"
                + "UPSERT INTO zbytes(vid,pos,val) "
                + "VALUES($vid,$pos,$val);", baseDir);
    }

    private static String buildUpsertFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "DECLARE $fparent AS Utf8;\n"
                + "DECLARE $fname AS Utf8;\n"
                + "DECLARE $vid AS Utf8;\n"
                + "UPSERT INTO zfile(fid,fparent,fname,vid) "
                + "VALUES($fid,$fparent,$fname,$vid)", baseDir);
    }

    private class VersionDao {
        final Session session;
        final TxControl tx;
        final String path;
        final String fid;
        String vid;
        boolean frozen = false;
        String author;
        String message;

        private VersionDao(Session session, TxControl tx, String path, String fid) {
            this.session = session;
            this.tx = tx;
            this.path = path;
            this.fid = fid;
        }

        private void createFile(List<byte[]> data) {
            final Path myPath = new Path(this.path);
            final Path dirPath = new Path(myPath, 1);
            String did = createFolder(session, tx, dirPath);
            String myVid = createVersion(data);
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$fparent", PrimitiveValue.newText(did),
                    "$fname", PrimitiveValue.newText(myPath.tail()),
                    "$vid", PrimitiveValue.newText(myVid)
            );
            session.executeDataQuery(queryUpsertFile, tx, params).join().getValue();
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
                    "$vid", PrimitiveValue.newText(vid)
            );
            session.executeDataQuery(queryCleanupVersion, tx, params).join().getValue();
        }

        private void upsertVersion(List<byte[]> data) {
            Params params = Params.of(
                    "$vid", PrimitiveValue.newText(vid),
                    "$fid", PrimitiveValue.newText(fid),
                    "$frozen", PrimitiveValue.newBool(frozen),
                    "$tv", PrimitiveValue.newTimestamp(Instant.now()),
                    "$author", PrimitiveValue.newText(author),
                    "$message", PrimitiveValue.newText(message)
            );
            session.executeDataQuery(queryUpsertVersion, tx, params).join().getValue();
            int pos = 0;
            for (byte[] b : data) {
                params = Params.of(
                        "$vid", PrimitiveValue.newText(vid),
                        "$pos", PrimitiveValue.newInt32(pos),
                        "$val", PrimitiveValue.newBytes(b)
                );
                // TODO: batch upsert
                session.executeDataQuery(queryUpsertBytes, tx, params).join().getValue();
            }
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
            List<String> tmp = Arrays.asList(path.split("/"));
            tmp.removeIf(v -> (v==null || v.length()==0));
            while (skip > 0 && tmp.size() > 0) {
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
    }

    public static class Folder implements Serializable {
        public final String id;
        public final String parent;
        public final String name;
        public final List<Folder> children = new ArrayList<>();
        public final List<File> files = new ArrayList<>();

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
