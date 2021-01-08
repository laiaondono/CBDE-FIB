package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {

	private int key2;

	public MyHBaseWriter_KeyDesign() {
		this.key = 0;
		this.reset();
		this.key2 = -1;
	}

	protected String nextKey() {
		// Dissenyem les keys amb els valors de "type" i "region" (Q2) per a identificar els valors que ens interessa llegir
		if (this.data.get("type").equals("type_3") && this.data.get("region").equals("0")) { //Q2
			++this.key2;
			return this.data.get("type") + "region_" + this.data.get("region") + "_" + this.key2;
		}
		// A les claus dels valors que no compleixen les condicions per a Q2 afegim una "_" a l'inici per a que s'emmagatzemin
		// a dalt de la taula. D'aquesta manera, sabrem on comencen els valors que ens interessa accedir per a la Q2 i també
		// sabrem que arriben fins al final de la taula
		else //no Q2
			return "_" + this.data.get("type") + "region_" + this.data.get("region") + "_" + this.key;

	}
		
}