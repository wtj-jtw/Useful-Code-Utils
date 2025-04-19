package testa;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import static java.util.Arrays.asList;

public class ArrowTableCreationUtil {

    public static Table createArrowTable()  {
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)),null);
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

        Schema schema = new Schema(asList(age, name));
        int rows = 10;
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

            return new Table(root);

        }
    }

    public static Table createArrowTable2()  {
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)),null);
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

        Schema schema = new Schema(asList(age, name));
        int rows = 10;
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector ageVector = (IntVector)root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        ageVector.allocateNew(rows);
        nameVector.allocateNew(rows);

        for (int i = 0; i < rows; i++) {
            ageVector.set(i, i);
            nameVector.set(i, ("Name " + i).getBytes());
        }
        root.setRowCount(rows);

        return new Table(root);
    }

    public static void main(String[] args) {
        Table arrowTable = createArrowTable2();
        System.out.println(arrowTable.contentToTSVString());
    }
}
