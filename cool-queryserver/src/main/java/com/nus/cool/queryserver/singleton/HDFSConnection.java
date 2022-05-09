package com.nus.cool.queryserver.singleton;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.schema.TableSchema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Initialize HDFS connection
 */
public class HDFSConnection {

    private static volatile HDFSConnection instance = null;

    private FileSystem fs;

    public static HDFSConnection getInstance() throws URISyntaxException, IOException {
        if (instance == null) {
            synchronized (HDFSConnection.class) {
                if (instance == null) {
                    instance = new HDFSConnection();
                }
            }
        }
        return instance;
    }

    private HDFSConnection() throws URISyntaxException, IOException {
        this.connect();
    }

    /**
     * Connect to hdfs
     * @throws URISyntaxException
     * @throws IOException
     */
    public void connect() throws URISyntaxException, IOException {
        System.out.println("connect to HDFS");
        String HDFS_HOST = "";
        try (InputStream input = new FileInputStream("conf/app.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            String ip = prop.getProperty("hdfs.host");
            HDFS_HOST = "hdfs://" + ip + ":9000";
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        fs = FileSystem.get(new URI(HDFS_HOST), new Configuration());
    }

    /**
     * Return contents of url assigned to each worker
     * @param source cubeName, data source name
     * @param queryId 0, 1 which kinds of query to run
     * @param queryType cohort / iceberg
     * @return list of contents, eg. cohort?path=hdfs://localhost:9000/cube/health/v1/&file=1805b2fdb75.dz&queryId=1
     * @throws IOException
     */
    public List<String> getParameters(String source, String queryId, String queryType) throws IOException {
        List<String> parameters = new ArrayList<>();
        FileStatus[] versions = fs.listStatus(new Path("/cube/" + source));
        for (FileStatus version : versions) {
            if (version.isDirectory()) {
                String path = version.getPath().toString();
                FileStatus[] files = fs.listStatus(version.getPath());
                for (FileStatus file : files) {
                    String[] tmp = file.getPath().toString().split("/");
                    String fileName = tmp[tmp.length - 1];
                    if (fileName.matches(".*[.]dz")) {
                        parameters.add("dist/" + queryType + "?path=" + path + "/&file=" + fileName + "&queryId=" + queryId);
                    }
                }
            }
        }
        return parameters;
    }

    /**
     * Read result from HDFS
     * @param queryId
     * @return
     * @throws IOException
     */
    public List<BaseResult> getResult(String queryId) throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/tmp/" + queryId + "/results"));
        List<BaseResult> raw = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (FileStatus status : statuses) {
            String content = mapper.readValue((InputStream) fs.open(status.getPath()), String.class);
            JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, BaseResult.class);
            List<BaseResult> results = mapper.readValue(content, javaType);
            raw.addAll(results);
        }
        raw = BaseResult.merge(raw);
        return raw;
    }

    /**
     * Get query resut from hdfs
     * @param queryId
     * @return
     * @throws IOException
     */
    public FileStatus[] getResults(String queryId) throws IOException {
        return fs.listStatus(new Path("/tmp/" + queryId + "/results"));
    }

    /**
     * Add query result from hdfs
     * @param queryId
     * @param content
     * @throws IOException
     */
    public void createResult(String queryId, String content) throws IOException {
        String resId = "res_"+UUID.randomUUID().toString();
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/results/" + resId));
        new ObjectMapper().writeValue((OutputStream) out, content);
    }

    /**
     * Create query file in hdfs
     * @param query
     * @return
     * @throws IOException
     */
    public String createQuery(IcebergQuery query) throws IOException {
        String queryId = UUID.randomUUID().toString();
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/query.json"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue((OutputStream) out, query);
        return queryId;
    }

    /**
     * Read icebergQuery from HDFS
     * @param queryId
     * @return
     * @throws IOException
     */
    public IcebergQuery readIcebergQuery(String queryId) throws IOException {
        return IcebergQuery.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    }

    /**
     * Read cohort query from HDFS
     * @param queryId
     * @return
     * @throws IOException
     */
    public ExtendedCohortQuery readCohortQuery(String queryId) throws IOException {
        return ExtendedCohortQuery.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    }


    /**
     * Copy local file to hdfs
     * @param localPath localPath
     * @param dfsPath localPath
     * @throws IOException
     */
    public void uploadToDfs(String localPath, String dfsPath) throws IOException {
        fs.copyFromLocalFile(new Path(localPath), new Path(dfsPath));
    }

    /**
     * Read cohort tableSchema from HDFS
     * @param path yaml path
     * @return table schema instance
     * @throws IOException
     */
    public TableSchema readTableSchema(String path) throws IOException {
        InputStream in = fs.open(new Path(path + "table.yaml"));
        return TableSchema.read(in);
    }

    /**
     * Create path for table yaml
     * @param path path for yaml file
     */
    public void createTableSchema(String path, TableSchema ts) throws IOException {
        FSDataOutputStream out = fs.create(new Path(path + "table.yaml"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue((OutputStream) out, ts);
    }

    /**
     * Read readCublet tableSchema from HDFS
     * @param path path
     * @param file file
     * @return ByteBuffer
     * @throws IOException
     */
    public ByteBuffer readCublet(String path, String file) throws IOException {
        // System.out.println("start read cublet");
        return ByteBuffer.wrap(IOUtils.toByteArray(fs.open(new Path(path + file))));
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        HDFSConnection fs = HDFSConnection.getInstance();

        String localPath1 = "/datasetSource/health/v00000002/1805b2fdb75.dz";
        String dfsPath1 = "/cube/health/v1/1805b2fdb75v2.dz";
        fs.uploadToDfs(localPath1, dfsPath1);
//
//        String localPath3 = "/Users/kevin/project_java/COOL/health/query2.json";
//        String dfsPath3 = "/tmp/1/query.json";
//        fs.uploadToDfs(localPath3, dfsPath3);

//        String localPath2 = "/Users/kevin/project_java/COOL/health/table.yaml";
//        String dfsPath2 = "/cube/health/v1/table.yaml";
//        fs.uploadToDfs(localPath2, dfsPath2);

//        ByteBuffer res = fs.readCublet("/health", "/1805b2fdb75.dz");
    }
}
