package testa;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.VectorSchemaRoot;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ArrowFileReaderUtil {
    public static void readArrowFile(){
        File file = new File("/Users/wangtianjie/Workspace/duckdbNivisTest/duckdb.arrow");
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                FileInputStream fileInputStream = new FileInputStream(file);
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
//            reader.loadNextBatch();
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
                System.out.printf("-----------------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        readArrowFile();
    }
}
