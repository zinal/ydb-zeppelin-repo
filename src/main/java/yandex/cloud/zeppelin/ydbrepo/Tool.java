package yandex.cloud.zeppelin.ydbrepo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Properties;

/**
 *
 * @author zinal
 */
public class Tool implements AutoCloseable {

    public static final String PROP_URL = "ydb.url";
    public static final String PROP_AUTH_MODE = "ydb.auth.mode";
    public static final String PROP_AUTH_DATA = "ydb.auth.data";
    public static final String PROP_DIR = "ydb.dir";
    public static final String PROP_FNAME_MANGLE = "ydb.fname.mangle";

    private final YdbFs fs;
    private final boolean fileNameMangling;

    public Tool(Properties config) {
        fs = new YdbFs(
                config.getProperty(PROP_URL),
                YdbFs.AuthMode.valueOf(config.getProperty(PROP_AUTH_MODE)),
                config.getProperty(PROP_AUTH_DATA),
                config.getProperty(PROP_DIR, "zeppelin")
        );
        fileNameMangling = Boolean.parseBoolean(config.getProperty(PROP_FNAME_MANGLE, "true"));
    }

    public void run(String command, String[] options) throws Exception {
        if ("import".equalsIgnoreCase(command)) {
            runImport(options);
        } else if ("export".equalsIgnoreCase(command)) {
            runExport(options);
        } else if ("rmdir".equalsIgnoreCase(command)) {
            runRmDir(options);
        } else if ("rmfile".equalsIgnoreCase(command)) {
            runRmFile(options);
        } else if ("checkpoint".equalsIgnoreCase(command)) {
            runCheckpoint(options);
        } else if ("history".equalsIgnoreCase(command)) {
            runHistory(options);
        } else if ("mvdir".equalsIgnoreCase(command)) {
            runMvDir(options);
        } else if ("mvfile".equalsIgnoreCase(command)) {
            runMvFile(options);
        } else {
            throw new IllegalArgumentException("Unsupported command: " + command);
        }
    }

    public void runImport(String[] options) throws Exception {
        for (String option : options) {
            runImport(option);
        }
    }

    public void runImport(String object) throws Exception {
        File f = new File(object);
        if (!f.exists() || !f.canRead()) {
            throw new IOException("File not found: " + object);
        }
        if (f.isFile()) {
            importFile("", f);
        } else if (f.isDirectory()) {
            for (File fx : f.listFiles(
                    (File dir1, String name) -> !(name.startsWith(".")))) {
                importObject("", fx);
            }
        } else {
            throw new IOException("Illegal file type: " + object);
        }
    }

    public void importObject(String basePath, String fname) throws Exception {
        importObject(basePath, new File(fname));
    }

    public void importObject(String basePath, File f) throws Exception {
        if (f.isFile()) {
            importFile(basePath, f);
        } else if (f.isDirectory()) {
            for (File fx : f.listFiles(
                    (File dir1, String name) -> !(name.startsWith(".")))) {
                importObject(basePath + "/" + f.getName(), fx);
            }
        }
    }

    public void importFile(String basePath, File f) throws Exception {
        String fullName = basePath + "/" + f.getName();
        String fid = null;
        if (fileNameMangling) {
            int dotIndex = fullName.lastIndexOf(".");
            int separatorIndex = fullName.lastIndexOf("_");
            if (separatorIndex > 0 && dotIndex > separatorIndex) {
                fid = fullName.substring(separatorIndex + 1, dotIndex);
                if (fid.length()==0)
                    fid = null;
                fullName = fullName.substring(0, separatorIndex);
            }
        }
        
        YdbFs.File desc = null;
        if (fid != null) {
            desc = fs.locateFile(fid);
        }
        if (desc == null) {
            desc = fs.locateFileByPath(fullName);
        }
        if (desc == null) {
            fid = YdbFs.newId();
        } else {
            fid = desc.id;
        }

        System.out.println("** IMPORT " + fullName);
        fs.saveFile(fid, fullName, "sys$system", Files.readAllBytes(f.toPath()));
    }

    public void runRmDir(String[] options) throws Exception {
        for (String option : options) {
            fs.removeFolder(option);
        }
    }

    public void runRmFile(String[] options) throws Exception {
        for (String path : options) {
            YdbFs.File file = fs.locateFileByPath(path);
            if (file==null) {
                System.out.println("** File not found: " + path);
            } else {
                System.out.println("** Removing file " + path + " -> " + file.id);
                fs.removeFile(file.id, path);
            }
        }
    }

    public void runMvDir(String[] options) throws Exception {
        if (options.length != 2)
            throw new IllegalArgumentException("Need exactly two arguments for mvdir");
        String srcPath = options[0];
        String dstPath = options[1];
        System.out.println("** MVDIR " + srcPath + " to " + dstPath);
        fs.moveFolder(srcPath, dstPath);
    }

    public void runMvFile(String[] options) throws Exception {
        if (options.length != 2)
            throw new IllegalArgumentException("Need exactly two arguments for mvfile");
        String srcPath = options[0];
        String dstPath = options[1];
        YdbFs.File file = fs.locateFileByPath(srcPath);
        if (file==null) {
            throw new IllegalArgumentException("Source file not found: " + srcPath);
        }
        System.out.println("** MVFILE " + file.id + " to " + dstPath);
        fs.moveFile(file.id, srcPath, dstPath);
    }

    public void runExport(String[] options) throws Exception {
        File targetDir = (options.length == 0) ? new File(".") : new File(options[0]);
        if (! targetDir.exists()) {
            targetDir.mkdirs();
        }
        if (! targetDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + targetDir);
        }
        YdbFs.FullList fullList = fs.listAll();
        for (YdbFs.File f : fullList.files.values()) {
            YdbFs.Path p = fullList.buildPath(f);
            if (p.isEmpty())
                continue;
            File dir = targetDir;
            if (p.entries.length > 1) {
                YdbFs.Path d = new YdbFs.Path(p, 1);
                for (String x : d.entries) {
                    dir = new File(dir, x);
                }
                dir.mkdirs();
            }
            File file = new File(dir, p.tail());
            System.out.println("** EXPORT " + file.getAbsolutePath());
            Files.write(file.toPath(), fs.readFile(f.id, null),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    public void runCheckpoint(String[] options) throws Exception {
        if (options.length == 0)
            return;
        String path = options[0];
        String message = (options.length > 1) ? options[1] : "?";
        String author = (options.length > 2) ? options[2] : "?";
        YdbFs.File file = fs.locateFileByPath(path);
        if (file==null) {
            throw new IllegalArgumentException("File not found: " + path);
        }
        String vid = fs.checkpoint(file.id, path, message, author, Instant.now());
        System.out.println("** CHECKPOINT " + path + " -> " + vid);
    }

    public void runHistory(String[] options) throws Exception {
        for (String path : options) {
            history(path);
        }
    }

    public void history(String path) throws Exception {
        YdbFs.File file = fs.locateFileByPath(path);
        if (file==null) {
            System.out.println("** File not found: " + path);
            return;
        }
        System.out.println("** History for " + path);
        System.out.println("** File id: " + file.id);
        for (YdbFs.Revision r : fs.listHistory(file.id)) {
            System.out.println("\t\t" + new java.util.Date(1000L * r.tv).toString()
                    + "\t" + r.author + "\t" + r.message);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("USAGE: Tool config.xml command ...");
            System.exit(2);
        }
        String[] options = new String[args.length - 2];
        System.arraycopy(args, 2, options, 0, options.length);
        try {
            Properties config = new Properties();
            try (FileInputStream fis = new FileInputStream(args[0])) {
                config.loadFromXML(fis);
            }
            try (Tool tool = new Tool(config)) {
                tool.run(args[1], options);
            }
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    @Override
    public void close() {
        fs.close();
    }

}
