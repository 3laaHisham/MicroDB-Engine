package exceptions;

public class DBSchemaException extends DBAppException {
    public DBSchemaException(String message) {
        super(message);
    }

    public DBSchemaException() {
        super();
    }

}
