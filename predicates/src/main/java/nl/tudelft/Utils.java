package nl.tudelft;

public class Utils {

    public static int seq(String message) {
        if (message.contains("LedgerSequence")) {
            message = message.substring(message.indexOf("LedgerSequence") + 14 + 2);
            message = message.substring(0, message.indexOf(","));
            return Integer.parseInt(message);
        }
        return -1;
    }

    public static int to(String message) {
        if (message.contains("->")) {
            message = message.substring(message.indexOf("->") + 2);
            message = message.substring(0, message.indexOf("]"));
            return Integer.parseInt(message);
        }
        return -1;
    }

}
