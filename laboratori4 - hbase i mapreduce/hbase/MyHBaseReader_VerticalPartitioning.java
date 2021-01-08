package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_VerticalPartitioning extends MyHBaseReader {

	protected String[] scanFamilies() {
		// Generem un String[] amb el nom de la família que ens interessa llegir per a Q1.
		// Aquesta família conté els atributs utilitzats per a la Q1: "type", "region" i "flav"
		String fam[] = {"fam1"};
		return fam;
	}
		
}

