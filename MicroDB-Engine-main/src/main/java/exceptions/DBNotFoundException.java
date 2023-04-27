package exceptions;

public class DBNotFoundException extends DBAppException {
    public DBNotFoundException(String message) {
        super(message);
    }

    public DBNotFoundException() {
        super();
    }

}
