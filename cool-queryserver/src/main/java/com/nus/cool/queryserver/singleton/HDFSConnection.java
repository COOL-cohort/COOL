package com.nus.cool.queryserver.singleton;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Initialize HDFS connection.
 */
public class HDFSConnection {

  private static volatile HDFSConnection instance = null;

  private FileSystem fs;

  /**
   * getInstance.
   */
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
   * Connect to hdfs.
   *
   * @throws URISyntaxException URISyntaxException
   * @throws IOException IOException
   */
  public void connect() throws URISyntaxException, IOException {
    System.out.println("connect to HDFS");

    ModelConfig.getInstance();
    String hdfsHost = "hdfs://" + ModelConfig.props.getProperty("hdfs.host") + ":9000";
    fs = FileSystem.get(new URI(hdfsHost), new Configuration());
  }

  /**
   * Return contents of url assigned to each worker.
   *
   * @param source    cubeName, data source name
   * @param queryId   0, 1 which kinds of query to run
   * @param queryType cohort / iceberg
   * @return list of contents,
   *     eg. cohort?path=hdfs://localhost:9000/cube/health/v1/&file=1805b2fdb75.dz&queryId=1
   * @throws IOException IOException
   */
  public List<String> getParameters(String source, String queryId, String queryType)
      throws IOException {
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
            parameters.add(
                "dist/" + queryType + "?path=" + path + "/&file=" + fileName + "&queryId="
                    + queryId);
          }
        }
      }
    }
    return parameters;
  }

  /**
   * Read result from HDFS.
   *
   * @param queryId queryId
   * @return result
   * @throws IOException IOException
   */
  public List<String> getResult(String queryId) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path("/tmp/" + queryId + "/results"));
    List<JavaType> raw = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (FileStatus status : statuses) {
      String content = mapper.readValue((InputStream) fs.open(status.getPath()), String.class);
      JavaType javaType =
          mapper.getTypeFactory().constructParametricType(ArrayList.class, JavaType.class);
      JavaType results = mapper.readValue(content, javaType);
    }
    return new ArrayList<>();
  }

  /**
   * Get query resut from hdfs.
   *
   * @param queryId queryId
   * @throws IOException IOException
   */
  public FileStatus[] getResults(String queryId) throws IOException {
    return fs.listStatus(new Path("/tmp/" + queryId + "/results"));
  }

  /**
   * Add query result from hdfs.
   *
   * @param queryId queryId
   * @param content content
   * @throws IOException IOException
   */
  public void createResult(String queryId, String content) throws IOException {
    String resId = "res_" + UUID.randomUUID().toString();
    FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/results/" + resId));
    new ObjectMapper().writeValue((OutputStream) out, content);
  }

  /**
   * Create query file in hdfs.
   *
   * @param query query
   * @throws IOException IOException
   */
  public String createQuery(OLAPQueryLayout query) throws IOException {
    String queryId = UUID.randomUUID().toString();
    FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/query.json"));
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue((OutputStream) out, query);
    return queryId;
  }

  /**
   * Read icebergQuery from HDFS.
   *
   * @param queryId queryId
   * @throws IOException IOException
   */
  public OLAPQueryLayout readIcebergQuery(String queryId) throws IOException {
    //    return OLAPQueryLayout.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    return new OLAPQueryLayout();
  }

  /**
   * Read cohort query from HDFS.
   *
   * @param queryId queryId
   * @throws IOException IOException
   */
  public CohortQueryLayout readCohortQuery(String queryId) throws IOException {
    //    return CohortQueryLayout.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    return new CohortQueryLayout();
  }


  /**
   * Copy local file to hdfs.
   *
   * @param localPath localPath
   * @param dfsPath   localPath
   * @throws IOException IOException
   */
  public void uploadToDfs(String localPath, String dfsPath) throws IOException {
    fs.copyFromLocalFile(new Path(localPath), new Path(dfsPath));
  }

  /**
   * Read cohort tableSchema from HDFS.
   *
   * @param path yaml path
   * @return table schema instance
   * @throws IOException IOException
   */
  public TableSchema readTableSchema(String path) throws IOException {
    InputStream in = fs.open(new Path(path + "table.yaml"));
    return TableSchema.read(in);
  }

  /**
   * Create path for table yaml.
   *
   * @param path path for yaml file
   */
  public void createTableSchema(String path, TableSchema ts) throws IOException {
    FSDataOutputStream out = fs.create(new Path(path + "table.yaml"));
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue((OutputStream) out, ts);
  }

  /**
   * Read readCublet tableSchema from HDFS.
   *
   * @param path path
   * @param file file
   * @return ByteBuffer
   * @throws IOException IOException
   */
  public ByteBuffer readCublet(String path, String file) throws IOException {
    // System.out.println("start read cublet");
    return ByteBuffer.wrap(IOUtils.toByteArray(fs.open(new Path(path + file))));
  }

  /**
   * Main func .
   *
   * @param args args
   * @throws URISyntaxException URISyntaxException
   * @throws IOException IOException
   */
  public static void main(String[] args) throws URISyntaxException, IOException {
    HDFSConnection fs = HDFSConnection.getInstance();

    String localPath1 =
        "/Users/kevin/project_java/COOL/datasetSource/health/v00000002/1807455469c.dz";
    String dfsPath1 = "/cube/health/v1/1805b2fdb75v2.dz";
    fs.uploadToDfs(localPath1, dfsPath1);

    String localPath5 =
        "/Users/kevin/project_java/COOL/datasetSource/health/v00000002/1807455469c.dz";
    String dfsPath5 = "/cube/health/v1/1805b2fdb75v1.dz";
    fs.uploadToDfs(localPath5, dfsPath5);

    String localPath3 = "/Users/kevin/project_java/COOL/health/query2.json";
    String dfsPath3 = "/tmp/1/query.json";
    fs.uploadToDfs(localPath3, dfsPath3);

    String localPath2 = "/Users/kevin/project_java/COOL/health/table.yaml";
    String dfsPath2 = "/cube/health/v1/table.yaml";
    fs.uploadToDfs(localPath2, dfsPath2);

    //        ByteBuffer res = fs.readCublet("/health", "/1805b2fdb75.dz");
  }
}
