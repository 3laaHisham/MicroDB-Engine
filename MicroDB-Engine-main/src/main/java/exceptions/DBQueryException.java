package exceptions;

public class DBQueryException extends DBAppException {
    public DBQueryException(String message) {
        super(message);
    }

    public DBQueryException() {
        super();
    }
}
