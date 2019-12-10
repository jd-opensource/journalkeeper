package io.journalkeeper.core;

/**
 * @author LiYue
 * Date: 2019/12/10
 */
public class Logo {
    public static final String LOGO = "\n" +
            "      _                              _   _  __                         \n" +
            "     | | ___  _   _ _ __ _ __   __ _| | | |/ /___  ___ _ __   ___ _ __ \n" +
            "  _  | |/ _ \\| | | | '__| '_ \\ / _` | | | ' // _ \\/ _ \\ '_ \\ / _ \\ '__|\n" +
            " | |_| | (_) | |_| | |  | | | | (_| | | | . \\  __/  __/ |_) |  __/ |   \n" +
            "  \\___/ \\___/ \\__,_|_|  |_| |_|\\__,_|_| |_|\\_\\___|\\___| .__/ \\___|_|   \n" +
            "                                                      |_|              \n";
    public static final String DOUBLE_LINE =  "=======================================================================\n";
    public static final String SINGLE_LINE =  "-----------------------------------------------------------------------\n";
    public static void main(String[] args) {
        String version = Logo.class.getPackage().getImplementationVersion();
        String str = "\n" +
                Logo.DOUBLE_LINE +
                Logo.LOGO +
                Logo.SINGLE_LINE +
                "Version: \t" + version + "\n" +
                Logo.DOUBLE_LINE;
        System.out.print(str);
    }
}
