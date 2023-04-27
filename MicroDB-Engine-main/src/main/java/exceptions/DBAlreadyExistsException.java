package exceptions;

public class DBAlreadyExistsException extends DBAppException {
    public DBAlreadyExistsException(String message) {
        super(message);
    }

    public DBAlreadyExistsException() {
        super();
    }

}
