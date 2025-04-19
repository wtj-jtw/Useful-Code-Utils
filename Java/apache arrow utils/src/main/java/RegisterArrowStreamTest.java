package testa;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.io.*;
import java.sql.*;
import java.util.Arrays;

/**
 * VM options:  --add-opens=java.base/java.nio=ALL-UNNAMED
 * <?xml version="1.0" encoding="UTF-8"?>
 * <project xmlns="http://maven.apache.org/POM/4.0.0"
 *          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *          xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
 *     <modelVersion>4.0.0</modelVersion>
 *
 *     <groupId>org.example</groupId>
 *     <artifactId>duckdbNivisTest</artifactId>
 *     <version>1.0-SNAPSHOT</version>
 *
 *     <properties>
 *         <maven.compiler.source>17</maven.compiler.source>
 *         <maven.compiler.target>17</maven.compiler.target>
 *         <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
 *     </properties>
 *     <dependencies>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-memory-netty</artifactId>
 *             <version>12.0.0</version>
 *             <scope>runtime</scope>
 *         </dependency>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-jdbc</artifactId>
 *             <version>12.0.0</version>
 *         </dependency>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>flight-core</artifactId>
 *             <version>12.0.0</version>
 *         </dependency>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-dataset</artifactId>
 *             <version>12.0.0</version>
 *         </dependency>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-c-data</artifactId>
 *             <version>12.0.0</version>
 *         </dependency>
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-memory</artifactId>
 *             <version>12.0.0</version>
 *             <type>pom</type>
 *         </dependency>
 *
 *         <dependency>
 *             <groupId>org.apache.arrow</groupId>
 *             <artifactId>arrow-vector</artifactId>
 *             <version>12.0.0</version>
 *         </dependency>
 *
 *         <dependency>
 *             <groupId>org.duckdb</groupId>
 *             <artifactId>duckdb_jdbc</artifactId>
 *             <version>1.0.0</version>
 *         </dependency>
 *     </dependencies>
 *
 * </project>
 * */
public class RegisterArrowStreamTest {
    public static void main(String[] args) throws Exception{
        run();
    }
    private static void run() throws Exception {
//        Schema schema = new Schema(asList(age, name), null);
        Field intField = new Field("intColumn", FieldType.nullable(new ArrowType.Int(32, true)), null);

        Schema schema = new Schema(Arrays.asList(intField));

        try(BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);){
            root.allocateNew();
            IntVector intVector = (IntVector) root.getVector("intColumn");
            // Add data
            intVector.setSafe(0, 1);
            intVector.setSafe(1, 2);
            intVector.setSafe(2, 3);
            intVector.setValueCount(3);

            // Set row count
            root.setRowCount(3);

            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.writeBatch();
                writer.end();
                byte[] arrowData = out.toByteArray();
                try (var duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duckDBConnection.createStatement()){
                    try (InputStream rawStream = new ByteArrayInputStream(arrowData);
                         ArrowStreamReader streamReader = new ArrowStreamReader(rawStream, allocator);
                         ArrowArrayStream arrowArrayStream = ArrowArrayStream.allocateNew(allocator)) {
                        Data.exportArrayStream(allocator, streamReader, arrowArrayStream);
                        duckDBConnection.registerArrowStream("tmp", arrowArrayStream);

                        ResultSet resultSet = statement.executeQuery("select count(*) from tmp");
                        while (resultSet.next()) {
                            System.out.println(resultSet.getInt(1));
                        }
                    }
                }
            }
        }
    }

}
