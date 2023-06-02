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

    // "hash":"842e65e52feb9e505d65ba4a8e3a03289908b92c07391f44c954a9a32f25edbe"
    public static String hash(String message) {
        if (message.contains("\"hash\"")) {
            message = message.substring(message.indexOf("\"hash\"") + 6 + 2);
            message = message.substring(0, message.indexOf("\""));
            return message;
        }
        return null;
    }

    public static int validatorNumber(String message) {
        if (message.contains("Validation")) {
            message = message.substring(message.indexOf("Validation") + 11);
            message = message.substring(0, message.indexOf(" "));
            return Integer.parseInt(message);
        }
        return -1;
    }

    public static String sendToSelf(String message) {
        return message.replace(message.substring(message.indexOf("->"), message.indexOf("->") + 3), "->" + validatorNumber(message));
    }

}
