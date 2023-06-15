package com.yandex.cloud.zeppelin.ydbrepo;

import io.micrometer.core.instrument.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.repo.NotebookRepoSettingsInfo;
import org.apache.zeppelin.notebook.repo.NotebookRepoWithVersionControl;
import org.apache.zeppelin.user.AuthenticationInfo;

/**
 *
 * @author mzinal
 */
public class YdbNotebookRepo implements NotebookRepoWithVersionControl {

    public static final String CONF_URL = "zeppelin.notebook.ydb.url";
    public static final String CONF_DIR = "zeppelin.notebook.ydb.dir";
    public static final String CONF_AUTH_MODE = "zeppelin.notebook.ydb.auth.mode";
    public static final String CONF_AUTH_DATA = "zeppelin.notebook.ydb.auth.data";

    private YdbFs fs;
    private Charset encoding;

    private static String getConfString(ZeppelinConfiguration zc, String prop, String val) {
        String v = zc.getString(prop.toUpperCase().replace('.', '_'), prop, val);
        if (v==null)
            return "";
        return v;
    }

    @Override
    public void init(ZeppelinConfiguration zc) throws IOException {
        if (fs != null)
            return;

        String url = getConfString(zc, CONF_URL, "grpc://127.0.0.1:2135?database=/local");
        String baseDir = getConfString(zc, CONF_DIR, "zeppelin");
        String authMode = getConfString(zc, CONF_AUTH_MODE, YdbFs.AuthMode.METADATA.name());
        String authData = getConfString(zc, CONF_AUTH_DATA, "");
        String encoding = zc.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING);

        if (baseDir.length() > 0 && !baseDir.endsWith("/"))
            baseDir = baseDir + "/";

        this.encoding = Charset.forName(encoding);

        try {
            this.fs = new YdbFs(url, YdbFs.AuthMode.valueOf(authMode), authData, baseDir);
        } catch(Exception ix) {
            throw new IOException("YDB connection failed", ix);
        }
    }

    @Override
    public void close() {
        if (fs!=null) {
            fs.close();
            fs = null;
        }
    }

    @Override
    public Map<String, NoteInfo> list(AuthenticationInfo subject) throws IOException {
        final Map<String, NoteInfo> retval = new HashMap<>();
        final YdbFs.FullList fl = fs.listAll();
        for (YdbFs.File file : fl.files.values()) {
            retval.put(file.id, new NoteInfo(file.id, fl.buildPath(file).toString()));
        }
        return retval;
    }

    @Override
    public Note get(String noteId, String notePath, AuthenticationInfo subject) throws IOException {
        byte[] data = fs.readFile(noteId);
        if (data==null)
            throw new FileNotFoundException(buildNoteFileName(noteId, notePath));
        String json = IOUtils.toString(new ByteArrayInputStream(data), encoding);
        Note note = Note.fromJson(noteId, json);
        note.setPath(notePath);
        return note;
    }

    @Override
    public void save(Note note, AuthenticationInfo subject) throws IOException {
        fs.saveFile(note.getId(), note.getPath(), subject.getUser(),
                note.toJson().getBytes(encoding));
    }

    @Override
    public void move(String noteId, String notePath, String newNotePath,
              AuthenticationInfo subject) throws IOException {
        fs.moveFile(noteId, notePath, newNotePath);
    }

    @Override
    public void move(String folderPath, String newFolderPath,
              AuthenticationInfo subject) throws IOException {
        fs.moveFolder(folderPath, newFolderPath);
    }

    @Override
    public void remove(String noteId, String notePath, AuthenticationInfo subject) throws IOException {
        fs.removeFile(noteId, notePath);
    }

    @Override
    public void remove(String folderPath, AuthenticationInfo subject) throws IOException {
        fs.removeFolder(folderPath);
    }

    @Override
    public List<NotebookRepoSettingsInfo> getSettings(AuthenticationInfo subject) {
        return new ArrayList<>();
    }

    @Override
    public void updateSettings(Map<String, String> settings, AuthenticationInfo subject) {
        // noop
    }

    @Override
    public Revision checkpoint(String noteId, String notePath, String checkpointMsg,
            AuthenticationInfo subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Note get(String noteId, String notePath, String revId, AuthenticationInfo subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public List<Revision> revisionHistory(String noteId, String notePath, AuthenticationInfo subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Note setNoteRevision(String noteId, String notePath, String revId, AuthenticationInfo subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
