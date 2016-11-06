package com.mbr.openc.localcache.impl;

import java.util.HashMap;
import java.util.Map;

import com.mbr.openc.localcache.DataXferInterface;
import com.mbr.openc.localcache.DataXferInterfaceImpl;
import com.mbr.openc.localcache.SimpleCacheDatasource;

public class SampleDatasourceImpl implements SimpleCacheDatasource {

	static Map<String, String> cacheMap = new HashMap();
	static boolean flip = false;

	static {
		cacheMap.put("JPM", "JPMorgan Chase & Co.  ");
		cacheMap.put("WFC", "Wells Fargo & Co	  ");
		cacheMap.put("KEY", "KeyCorp		  ");
		cacheMap.put("PSX", "Phillips 66	     	  ");
		cacheMap.put("MPC", "Marathon Petroleu...  ");
		cacheMap.put("VLO", "Valero Energy Cor...  ");
		cacheMap.put("TSO", "Tesoro Corporation    ");
		cacheMap.put("HFC", "HollyFrontier Corp    ");
		cacheMap.put("DK", "Delek US Holdings...   ");
		cacheMap.put("NTI", "Northern Tier Ene...  ");
		cacheMap.put("PBF", "PBF Energy Inc	  ");
		cacheMap.put("FITB", "Fifth Third Bancorp  ");
		cacheMap.put("BNPQY", "BNP Paribas SA (ADR)");
		cacheMap.put("BEN", "Franklin Resource...  ");
		cacheMap.put("PSX", "Phillips 66	     	  ");
		cacheMap.put("MPC", "Marathon Petroleu...  ");
		cacheMap.put("VLO", "Valero Energy Cor...  ");
		cacheMap.put("TSO", "Tesoro Corporation    ");
		cacheMap.put("HFC", "HollyFrontier Corp    ");
		cacheMap.put("DK", "Delek US Holdings...   ");
		cacheMap.put("NTI", "Northern Tier Ene...  ");
		cacheMap.put("PBF", "PBF Energy Inc	  ");
		cacheMap.put("CLMT", "Calumet Specialty... ");
		cacheMap.put("WNR", "Western Refining,...  ");
		cacheMap.put("ALDW", "Alon USA Partners LP ");
		cacheMap.put("LMLP", "ML Payment Systems,c.");
		cacheMap.put("RAVN", "aven Industries, Inc.");
		cacheMap.put("ALG", "lamo Group, Inc.");
		cacheMap.put("PODD", "nsulet Corporation");
		cacheMap.put("AMSC", "merican Superconductor");
		cacheMap.put("TK", "eekay Corporation");
		cacheMap.put("TTC", "oro Co");
		cacheMap.put("KEM", "EMET Corporation");
		cacheMap.put("GEOS", "eospace Tech. Corp.");
		cacheMap.put("NCS", "CI Building Systems nc");
		cacheMap.put("GE", "eneral Electric Compny");
		cacheMap.put("BA", "oeing Co");
		cacheMap.put("DE", "eere & Company");
		cacheMap.put("CAT", "aterpillar Inc.");
		cacheMap.put("DAL", "elta Air Lines, Inc.");
		cacheMap.put("PACB", "acific Biosciences of");
		cacheMap.put("TROV", "rovaGene Inc");
		cacheMap.put("SRPT", "arepta Therapeutics nc");
		cacheMap.put("ARRY", "rray Biopharma Inc");
		cacheMap.put("MNOV", "ediciNova, Inc.");
		cacheMap.put("RGEN", "epligen Corporation");
		cacheMap.put("OCLR", "claro, Inc.");
		cacheMap.put("CYTX", "ytori Therapeutics Inc");
		cacheMap.put("ALKS", "lkermes Plc");
		cacheMap.put("VRX", "aleant Pharmaceuticals");
		cacheMap.put("ANAC", "nacor Pharmaceuticals");
		cacheMap.put("BAX", "axter International nc");
		cacheMap.put("JNJ", "ohnson & Johnson");
		cacheMap.put("VRX", "aleant Pharmaceuticals");
		cacheMap.put("GILD", "ilead Sciences, Inc.");
		cacheMap.put("BCOM", " Communications Ltd");
		cacheMap.put("LMOS", "umos Networks Corp");
		cacheMap.put("CTL", "enturylink Inc");
		cacheMap.put("CBB", "incinnati Bell Inc.");
		cacheMap.put("NQ", "Q Mobile Inc (ADR");
		cacheMap.put("TEO", "elecom Argentina SA ");
		cacheMap.put("VG", "onage Holdings Corp.");
		cacheMap.put("TLK", "elekomunikasi Indones");
		cacheMap.put("GNCMA", "eneral Communication");
		cacheMap.put("T", "AT&T Inc.");
		cacheMap.put("VZ", "erizon Communications");
		cacheMap.put("CMCSA", "omcast Corporation");
		cacheMap.put("CTL", "enturylink Inc");
		cacheMap.put("VOD", "odafone Group Plc ADR)");

	}

	public SampleDatasourceImpl() {

	}

	/**
	 * NOTE:
	 * -------------------------------------------------------------------------
	 * ------
	 * -------------------------------------------------------------------------
	 * ------------ N O T ---- T H R E A D S A F E
	 * -------------------------------------------------------------------------
	 * ------------
	 * 
	 */
	@Override
	public DataXferInterface getData(DataXferInterface request) throws Exception {

		Thread.sleep(2000);
		System.out.println("Datasource serving request [" + request.getRequest() + "]");
		if (cacheMap.containsKey((String) request.getRequest())) {
			return new DataXferInterfaceImpl(request.getRequest(), cacheMap.get(request.getRequest()));
		}
		return new DataXferInterfaceImpl(request.getRequest(), "NOT AVAILABLE");
	}

}
