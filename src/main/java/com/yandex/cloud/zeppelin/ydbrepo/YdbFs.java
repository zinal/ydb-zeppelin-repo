package com.yandex.cloud.zeppelin.ydbrepo;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final String queryGetFile;

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
        this.queryGetFile = buildGetFile(baseDir);
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
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
                count = session.executeDataQuery(queryGetFile, tx, params)
                        .thenApply(Result::getValue)
                        .thenApply(result -> {
                            int localCount = 0;
                            ResultSetReader rsr = result.getResultSet(0);
                            while (rsr.next()) {
                                // MAYBE: move decompression out of transaction
                                decompress(rsr.getColumn(0).getBytes(), baos);
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
        return baos.toByteArray();
    }

    private static String buildGetFile(String baseDir) {
        return String.format("PRAGMA TablePathPrefix('%s');\n"
                + "DECLARE $fid AS Utf8;\n"
                + "DECLARE $pos AS Int32;\n"
                + "DECLARE $limit AS Int32;\n"
                + "SELECT b.val, b.pos "
                + "FROM (SELECT vid FROM zfile WHERE fid=$fid) AS f "
                + "INNER JOIN zver v ON v.vid=f.vid "
                + "INNER JOIN zbytes b ON b.bid=v.bid "
                + "WHERE b.pos > $pos "
                + "ORDER BY b.pos LIMIT $limit", baseDir);
    }

    private static AuthProvider makeStaticAuthProvider(String authData) {
        int pos = authData.indexOf(":");
        if (pos < 0)
            return new StaticCredentials(authData, "");
        return new StaticCredentials(authData.substring(0, pos), authData.substring(pos+1));
    }

    private static void compress(byte[] input, ByteArrayOutputStream output) {
        if (input==null || input.length==0)
            return;
        final Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);
        deflater.setInput(input);
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

    public static enum AuthMode {
        METADATA,
        SAKEY,
        STATIC,
        NONE,
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

        public String buildPath(File file) {
            String path = file.name;
            Folder folder = folders.get(file.parent);
            while (folder != null && folder.isLeaf() ) {
                path = folder.name + "/" + path;
                folder = folders.get(folder.parent);
            }
            return path;
        }
    }

}
