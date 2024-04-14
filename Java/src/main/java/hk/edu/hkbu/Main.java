package hk.edu.hkbu;

import java.text.DecimalFormat;

public class Main {

    public static void main(String[] args) {

        if (args != null && args.length >= 1 && args[0] != null && args[0].equals("PPR")) {
            PersonalizedPageRank.main(args);
        } else {
            CommonFriends.main(args);
        }
    }
}
