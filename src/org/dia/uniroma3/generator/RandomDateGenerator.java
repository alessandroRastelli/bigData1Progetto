package org.dia.uniroma3.generator;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class RandomDateGenerator {
    
    public static String generate() {

        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(2015, 2015);

        gc.set(Calendar.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR));

        gc.set(Calendar.DAY_OF_YEAR, dayOfYear);

        return (gc.get(Calendar.YEAR) + "-" + gc.get(Calendar.MONTH) + "-" + gc.get(Calendar.DAY_OF_MONTH));

    }

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
}
