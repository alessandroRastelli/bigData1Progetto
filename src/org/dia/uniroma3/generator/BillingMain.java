package org.dia.uniroma3.generator;

import java.io.IOException;

public class BillingMain {
	
public static void main(String[] args) throws IOException {
		
		// Comando da usare per generare il dataset
		
		// prima bisogna configurare il generatore di scontrini
		/* al costruttore va passato un file che contiene in chiaro la lista di
		 * cibi da cui pescare
		 * 
		 * tale file è nella cartella billing
		 * il file food può tranquillamente essere editato aggiungendo nuovi
		 * cibi (uno per riga)
		 */

		ItemBillingGenerator IB = new ItemBillingGenerator("data/generator/billing/food.txt");
		
		/* quindi bisogna richiamare la funzione generate in cui bisogna passare:
		 * - il nome del file in cui generare il dataset
		 * - il numero di righe del file (nell'esempio 10)
		 * - il numero massimo di cibi per scontrino (nell'esempio 5)
		 * - la data viene generata in modo randomico nel formato yyyy-mm-dd
		 */
		IB.generate("data/generator/sample/esempio.txt", 10, 10);
		
		// big big big data!
		System.out.println("start1");
		IB.generate("data/generator/sample/esempio-10K.txt", (int) Math.pow(10, 4), 10);
		System.out.println("start2");
		IB.generate("data/generator/sample/esempio-1M.txt", (int) Math.pow(10, 6), 10);
		System.out.println("start3");
		IB.generate("data/generator/sample/esempio-10M.txt", (int) Math.pow(10, 7), 10);
		System.out.println("start4");
		IB.generate("data/generator/sample/esempio-50M.txt", (int) (5 * Math.pow(10, 7)), 10);
		System.out.println("start5");
		IB.generate("data/generator/sample/esempio-100M.txt", (int) Math.pow(10, 8), 10);
		System.out.println("done");
	}

}
