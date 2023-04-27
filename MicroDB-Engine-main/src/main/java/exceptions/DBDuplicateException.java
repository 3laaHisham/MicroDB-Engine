package exceptions;

public class DBDuplicateException extends DBAppException {
    public DBDuplicateException(String message) {
        super(message);
    }

    public DBDuplicateException() {
        super();
    }
}
