package testa;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static java.util.Arrays.asList;

public class ArrowFileWriterUtil {

    public static void createArrowFile() throws IOException {
        File file = new File("duckdb.arrow");
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)),null);
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

        Schema schema = new Schema(asList(age, name));
        int rows = 100;
        try(BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)
        ){
            IntVector ageVector = (IntVector)root.getVector("age");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            ageVector.allocateNew(rows);
            nameVector.allocateNew(rows);

            for (int i = 0; i < rows; i++) {
                ageVector.set(i, i);
                nameVector.set(i, ("Name " + i).getBytes());
            }
            root.setRowCount(rows);

            try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                 ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())
            ){
                writer.start();
                writer.writeBatch();
                writer.end();
                writer.close();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + root.getRowCount());
            }

        }
    }

    public static void main(String[] args) {
        try {
            createArrowFile();
        }catch (Exception e){
        }
    }

}
