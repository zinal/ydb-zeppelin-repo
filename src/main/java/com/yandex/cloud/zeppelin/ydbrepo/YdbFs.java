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

    public FullList listAll() {
        return new FullList(listFolders(), listFiles()).fill();
    }

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

    public boolean saveFile(String fid, String path, String author, byte[] data) {
        // Compression moved out of transaction
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

        final TxControl tx = TxControl.serializableRw();
        final PrimitiveValue vfid = PrimitiveValue.newText(fid);

        return retryContext.supplyResult(session -> {
            final VersionDao vc = new VersionDao(session, tx, path, fid);
            vc.frozen = false;
            vc.author = author;
            vc.message = "-";

            Params params = Params.of("$fid", vfid);
            ResultSetReader rsr = session.executeDataQuery(queryCheckFile, tx, params)
                    .thenApply(Result::getValue)
                    .thenApply(result -> result.getResultSet(0)).join();
            final boolean retval = rsr.next();
            if (retval) {
                final boolean frozenNow = rsr.getColumn(1).getBool();
                if (frozenNow) {
                    vc.createVersion(compressed);
                } else {
                    vc.vid = rsr.getColumn(0).getText();
                    vc.replaceVersion(compressed);
                }
            } else {
                vc.createFile(compressed);
            }
            return CompletableFuture.completedFuture(Result.success(retval));
        }).join().getValue();
    }

    private static String buildCheckFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "SELECT v.vid, v.frozen"
                + "FROM (SELECT vid FROM zfile WHERE fid=$fid) AS f "
                + "INNER JOIN zver v ON v.vid=f.vid", baseDir);
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
                throw new RuntimeException("Path not found: " + path.toString());
            }
            dparent = rsr.getColumn(0).getText();
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

    private static AuthProvider makeStaticAuthProvider(String authData) {
        int pos = authData.indexOf(":");
        if (pos < 0)
            return new StaticCredentials(authData, "");
        return new StaticCredentials(authData.substring(0, pos), authData.substring(pos+1));
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
            final Path path = new Path(this.path);
            String did = locateFolder(session, tx, path);
            String vid = createVersion(data);
            Params params = Params.of(
                    "$fid", PrimitiveValue.newText(fid),
                    "$fparent", PrimitiveValue.newText(did),
                    "$fname", PrimitiveValue.newText(path.entries[path.entries.length - 1]),
                    "$vid", PrimitiveValue.newText(vid)
            );
            session.executeDataQuery(queryUpsertFile, tx, params);
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
            session.executeDataQuery(queryCleanupVersion, tx, params);
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
            session.executeDataQuery(queryUpsertVersion, tx, params);
            int pos = 0;
            for (byte[] b : data) {
                params = Params.of(
                        "$vid", PrimitiveValue.newText(vid),
                        "$pos", PrimitiveValue.newInt32(pos),
                        "$val", PrimitiveValue.newBytes(b)
                );
                // TODO: batch upsert
                session.executeDataQuery(queryUpsertBytes, tx, params);
            }
        }

    }

    public static enum AuthMode {
        METADATA,
        SAKEY,
        STATIC,
        NONE,
    }

    public static class Path {
        public final String[] entries;

        public Path(String[] entries) {
            this.entries = entries;
        }

        public Path(String path) {
            List<String> tmp = Arrays.asList(path.split("/"));
            tmp.removeIf(v -> (v==null || v.length()==0));
            this.entries = tmp.toArray(new String[0]);
        }

        // reverse-list constructor
        public Path(List<String> elements) {
            final int n = elements.size();
            this.entries = new String[n];
            for (int i=0; i<n; ++i) {
                this.entries[i] = elements.get(n-(i+1));
            }
        }

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

        public File(String id, String parent, String name, String version) {
            this.id = id;
            this.parent = parent;
            this.name = name;
            this.version = version;
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
