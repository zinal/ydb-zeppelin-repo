package com.yandex.cloud.zeppelin.ydbrepo;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;

/**
 *
 * @author zinal
 */
public class Tool {

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
        } else {
            throw new IllegalArgumentException("Unsupported command: " + command);
        }
    }

    public void runImport(String[] options) throws Exception {
        for (String option : options) {
            importObject("", option);
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
            fid = UUID.randomUUID().toString();
        } else {
            fid = desc.id;
        }

        fs.saveFile(fid, fullName, "sys$system", Files.readAllBytes(f.toPath()));
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
            new Tool(config).run(args[1], options);
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
