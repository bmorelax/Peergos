package peergos.shared.io.ipfs.api;

import java.io.*;
import java.net.*;
import java.util.*;

public class Multipart {
    private final String boundary;
    private static final String LINE_FEED = "\r\n";
    private String charset;
    private ByteArrayOutputStream cache;
    private PrintWriter writer;
    private final URL target;

    public Multipart(String requestURL, String charset) throws IOException {
        this.charset = charset;

        boundary = createBoundary();

        this.target = new URL(requestURL);
        cache = new ByteArrayOutputStream();
        writer = new PrintWriter(new OutputStreamWriter(cache, charset), true);
        System.out.println("POSTER: CREATING MPC Thread:"  + Thread.currentThread().getId() + " to " + requestURL);
    }

    public static String createBoundary() {
        Random r = new Random();
        String allowed = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder b = new StringBuilder();
        for (int i=0; i < 32; i++)
            b.append(allowed.charAt(r.nextInt(allowed.length())));
        return b.toString();
    }

    public void addFormField(String name, String value) {
        writer.append("--" + boundary).append(LINE_FEED);
        writer.append("Content-Disposition: form-data; name=\"" + name + "\"")
                .append(LINE_FEED);
        writer.append("Content-Type: text/plain; charset=" + charset).append(
                LINE_FEED);
        writer.append(LINE_FEED);
        writer.append(value).append(LINE_FEED);
        writer.flush();
    }

    /** Recursive call to add a subtree to this post
     *
     * @param path
     * @param dir
     * @throws IOException
     */
    public void addSubtree(String path, File dir) throws IOException {
        String dirPath = path + (path.length() > 0 ? "/" : "") + dir.getName();
        addDirectoryPart(dirPath);
        for (File f: dir.listFiles()) {
            if (f.isDirectory())
                addSubtree(dirPath, f);
            else
                addFilePart("file", new NamedStreamable.FileWrapper(dirPath + "/", f));
        }
    }

    public void addDirectoryPart(String path) {
        try {
            writer.append("--" + boundary).append(LINE_FEED);
            writer.append("Content-Disposition: file; filename=\"" + URLEncoder.encode(path, "UTF-8") + "\"").append(LINE_FEED);
            writer.append("Content-Type: application/x-directory").append(LINE_FEED);
            writer.append("Content-Transfer-Encoding: binary").append(LINE_FEED);
            writer.append(LINE_FEED);
            writer.append(LINE_FEED);
            writer.flush();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void addFilePart(String fieldName, NamedStreamable uploadFile) throws IOException {
        Optional<String> fileName = uploadFile.getName();
        writer.append("--" + boundary).append(LINE_FEED);
        if (!fileName.isPresent())
            writer.append("Content-Disposition: file; name=\"" + fieldName + "\";").append(LINE_FEED);
        else
            writer.append("Content-Disposition: file; filename=\"" + fileName.get() + "\"").append(LINE_FEED);
        writer.append("Content-Type: application/octet-stream").append(LINE_FEED);
        writer.append("Content-Transfer-Encoding: binary").append(LINE_FEED);
        writer.append(LINE_FEED);
        writer.flush();

        InputStream inputStream = uploadFile.getInputStream();
        byte[] buffer = new byte[4096];
        int r;
        while ((r = inputStream.read(buffer)) != -1)
            cache.write(buffer, 0, r);
        cache.flush();
        inputStream.close();

        writer.append(LINE_FEED);
        writer.flush();
    }

    public void addHeaderField(String name, String value) {
        writer.append(name + ": " + value).append(LINE_FEED);
        writer.flush();
    }

    public String finish() throws IOException {
        StringBuilder b = new StringBuilder();

        System.out.println("POSTER: FINISHING MULTIPART POST Thread:"  + Thread.currentThread().getId());
        try {
            writer.append("--" + boundary + "--").append(LINE_FEED);
            writer.flush();
            byte[] requestBody = cache.toByteArray();
            HttpURLConnection httpConn = (HttpURLConnection) target.openConnection();
            httpConn.setUseCaches(false);
            httpConn.setDoOutput(true);
            httpConn.setDoInput(true);
//            httpConn.setRequestProperty("Expect", "100-continue");
            httpConn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
            httpConn.setRequestProperty("User-Agent", "Java IPFS Client");
            httpConn.setRequestProperty("Connection", "close");
            httpConn.setRequestProperty("Content-Length", ""+requestBody.length);
            OutputStream out = httpConn.getOutputStream();
            out.write(requestBody);
            out.flush();
            int status = httpConn.getResponseCode();
            System.out.println("POSTER: GETTING RESPONSE CODE " + Thread.currentThread().getId());
            if (status == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        httpConn.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    b.append(line);
                }
                System.out.println("POSTER: CLOSING MPC INPUTSTREAM "+ Thread.currentThread().getId());
                reader.close();
                httpConn.disconnect();
            } else {
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                            httpConn.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        b.append(line);
                    }
                    System.out.println("POSTER: CLOSING MPC INPUTSTREAM "+ Thread.currentThread().getId());
                    reader.close();
                } catch (Throwable t) {
                }
                throw new IOException("Server returned status: " + status + " with body: " + b.toString() + " and Trailer header: " + httpConn.getHeaderFields().get("Trailer"));
            }

            return b.toString();
        } catch (Exception ex) {
            System.out.println("THIS EXCEPTION IS  BEING SQUASHED ");
            throw new IllegalStateException("THIS EXCEPTION IS  BEING SQUASHED ", ex);
        }
    }
}
