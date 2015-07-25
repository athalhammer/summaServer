package edu.kit.aifb.summa;

import java.util.ArrayList;
import java.util.LinkedList;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;

import edu.kit.aifb.summa.model.Property;
import edu.kit.aifb.summa.model.TripleMeta;
import edu.kit.aifb.summa.model.URI;
import edu.kit.aifb.summa.model.TripleMeta.TripleFocus;


/**
 * This is an example summarization approach that generates summaries with
 * the DBpedia SPARQL endpoint.
 *
 */
public class SimpleSummarizer implements Summarizer {
	
	private static final String REPOSITORY = "http://dbpedia.org/sparql";
	
	private static final String QUERY_0 = "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
			+ "SELECT DISTINCT ?l FROM <http://dbpedia.org> WHERE { "
			+ "OPTIONAL {<ENTITY> <http://www.w3.org/2000/01/rdf-schema#label> ?l ."
			+ "FILTER regex(lang(?l), \"LANG\", \"i\") . }}";
	
	private static final String QUERY_1 = "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
			+ "SELECT DISTINCT ?o ?l "
			+ "FROM <http://dbpedia.semanticmultimedia.org/dbpedia2014/en/pageinlinkCounts_en_2014.ttl.bz2> "
			+ "FROM <http://dbpedia.org> WHERE"
			+ "{<ENTITY> ?p ?o . ?o dbpedia-owl:wikiPageInLinkCountCleaned ?pageInLinkCount."
			+ "PREDICATES"
			+ "OPTIONAL {?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "
			+ "FILTER regex(lang(?l), \"LANG\", \"i\") .}}"
			+ "ORDER BY DESC (?pageInLinkCount) LIMIT TOPK";
	
	private static final String QUERY_2 = "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
			+ "SELECT ?p ?l ?rank "
			+ "FROM <http://dbpedia.semanticmultimedia.org/dbpedia2014/en/pageinlinkCounts_en_2014.ttl.bz2> "
			+ "FROM <http://dbpedia.org> WHERE {"
			+ "<ENTITY> ?p <OBJECT> ."
			+ "<OBJECT> dbpedia-owl:wikiPageInLinkCountCleaned ?rank ."
			+ "OPTIONAL {?p <http://www.w3.org/2000/01/rdf-schema#label> ?l."
			+ "FILTER regex(lang(?l), \"LANG\", \"i\")} } ORDER BY asc(?p)";
	
	public LinkedList<TripleMeta> summarize(java.net.URI uri, String[] fixedProperties,
			Integer topK, Integer maxHops, String language) {
		HTTPRepository rep = new HTTPRepository(REPOSITORY);
		if (language == null) {
			language = "en";
		}
		
		RepositoryConnection con = null;

		 
		LinkedList<TripleMeta> result = new LinkedList<TripleMeta>();
		try {
			con = rep.getConnection();
			TupleQuery q1 = con.prepareTupleQuery(QueryLanguage.SPARQL, 
					QUERY_0.replace("ENTITY", uri.toString()).replace("LANG", language));
			TupleQueryResult r1 = q1.evaluate();
			URI subject = null;
			if (r1.hasNext()) {
				BindingSet set = r1.next();
				Binding l = set.getBinding("l");
				
				if (l == null) {
					subject = new URI(uri);
				} else {
					subject = new URI(uri, l.getValue().stringValue());
				}
				
			}
			r1.close();
			String query1 = QUERY_1.replace("ENTITY", uri.toString()).
					replace("LANG", language).
					replace("TOPK", Integer.toString(topK));
			if (fixedProperties.length > 0) {
				String replacement = "FILTER (";
				for (String string : fixedProperties) {
					replacement += "?p = <" + string + "> || ";
				}
				replacement = replacement.substring(0, replacement.length() - 3);
				replacement += ") .";
				query1 = query1.replaceAll("PREDICATES", replacement);
			} else {
				query1 = query1.replaceAll("PREDICATES", "");
			}
			TupleQuery q2 = con.prepareTupleQuery(QueryLanguage.SPARQL, query1);

			TupleQueryResult r2 = q2.evaluate();
			ArrayList<URI> objects = new ArrayList<URI>(); 
			while (r2.hasNext()) {
				BindingSet set = r2.next();
				Binding o = set.getBinding("o");
				Binding l = set.getBinding("l");
				URI object = null;
				if (l == null) {
					object = new URI(new java.net.URI(o.getValue().toString()));
				} else {
					object = new URI(new java.net.URI(o.getValue().toString()), l.getValue().stringValue());
				}

				objects.add(object);
			}
			r2.close();

			for (URI object : objects) {
				TupleQuery q3 = con.prepareTupleQuery(QueryLanguage.SPARQL, 
						QUERY_2.replace("ENTITY", uri.toString()).
						replace("LANG", language).
						replace("OBJECT", object.getURI().toString()));
				TupleQueryResult r3 = q3.evaluate();
				if (r3.hasNext()) {
					BindingSet set = r3.next();
					Binding p = set.getBinding("p");
					Binding l = set.getBinding("l");
					Binding rank = set.getBinding("rank");
					
					Property predicate = null;
					if (l == null) {
						predicate = new Property(new java.net.URI(p.getValue().stringValue()));

					} else {
						predicate = new Property(new java.net.URI(p.getValue().stringValue()),
								l.getValue().stringValue());						
					}

					TripleMeta meta = new TripleMeta(subject, predicate, object, TripleFocus.subject);
					meta.setRank(new Double(rank.getValue().stringValue()));
					result.add(meta);
				}
				r3.close();
 			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
