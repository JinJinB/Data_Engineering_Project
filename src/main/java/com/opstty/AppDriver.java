package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class,
                    "A map/reduce program that lists the districts that contain trees.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists the tree species.");
            programDriver.addClass("speciescount", SpeciesCount.class,
                    "A map/reduce program that gives the number of trees per kind.");
            programDriver.addClass("maxheight", MaxHeight.class,
                    "A map/reduce program that gives the height of the tallest tree per kind.");
            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that sorts the trees by height.");
            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that displays the oldest tree's district.");
            programDriver.addClass("districtcount", DistrictCount.class,
                    "A map/reduce program that displays the district that conains the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
