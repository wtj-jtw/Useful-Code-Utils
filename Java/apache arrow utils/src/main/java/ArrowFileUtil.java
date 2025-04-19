package testa;

public class ArrowFileUtil {
    public static void main(String[] args) {
        try {
            ArrowFileWriterUtil.createArrowFile();
            ArrowFileReaderUtil.readArrowFile();
        }catch (Exception e){

        }
    }
}
