package edu.kit.aifb.summa.servlet;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;

import edu.kit.aifb.summa.SimpleSummarizer;
import edu.kit.aifb.summa.model.TripleMeta;
import edu.kit.aifb.summa.model.URI;


@Path("")
public class JerseyService {
	
	// path of this service
	private static final String PATH = "http://km.aifb.kit.edu/summaServer/sum";
	
	// SUMMA vocabulary
	private static final String SUMMARY = "http://purl.org/voc/summa/Summary";
	private static final String ENTITY = "http://purl.org/voc/summa/entity";
	private static final String TOP_K = "http://purl.org/voc/summa/topK";
	private static final String MAX_HOPS = "http://purl.org/voc/summa/maxHops";
	private static final String LANGUAGE = "http://purl.org/voc/summa/language";
	private static final String STATEMENT = "http://purl.org/voc/summa/statement";
	private static final String FIXED_PROPERTY = "http://purl.org/voc/summa/fixedProperty";
	
	// vrank vocabulary
	private static final String HAS_RANK = "http://purl.org/voc/vrank#hasRank";
	private static final String RANK_VALUE = "http://purl.org/voc/vrank#rankValue";
	
	@Context
	private ServletContext context;

	
	/**
	 * 
	 * 
	 * @param message
	 * @param inputMime, curl standard is application/x-www-form-urlencoded
	 * @param outputMime, standard is everything
	 /** @return
	 *
	 */
	@POST
	public Response getPost(String message,
			@HeaderParam("Content-Type") String inputMime,
			@HeaderParam("Accept") String outputMime) {

		// get formats for MIME types
		RDFFormat inputFormat = Rio.getParserFormatForMIMEType(inputMime);
		RDFFormat outputFormat = Rio.getParserFormatForMIMEType(outputMime.split(",")[0]);
		if (inputFormat == null) {
			// TODO
			// method which detects the format
			// if Format could not be detected leave method and return error 
		}
		
		if (outputFormat == null) {
			outputFormat = RDFFormat.TURTLE;
		}
		
		try {
			Model model = Rio.parse(new StringReader(message), "", inputFormat);
			
			ValueFactory f = ValueFactoryImpl.getInstance();
			String entity = model.filter(null, f.createURI(ENTITY), null).objectURI().stringValue();
			Integer topK = Integer.parseInt(model.filter(null, f.createURI(TOP_K), null).objectValue().stringValue());

			Model maxHopsMod = model.filter(null, f.createURI(MAX_HOPS), null);
			Integer maxHops = null;
			if (!maxHopsMod.isEmpty()) {
				maxHops = Integer.parseInt(maxHopsMod.objectValue().stringValue());
			}

			String language = null;
			Model languageMod = model.filter(null, f.createURI(LANGUAGE), null);
			if (!languageMod.isEmpty()) {
				language = languageMod.objectValue().stringValue();
			}
			Model m = model.filter(null, f.createURI(FIXED_PROPERTY), null);
			Set<Value> objects = m.objects();
			Iterator<Value> val = objects.iterator();
			String [] fixedProperties = new String [objects.size()];
			for (int i = 0; i < fixedProperties.length; i++) {
				fixedProperties[i] = val.next().stringValue();
			}
			
			return executeQuery(entity, topK, maxHops, fixedProperties, language, outputFormat);

		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (UnsupportedRDFormatException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@GET
    public Response getRDF(@QueryParam("entity") String entity,
    		@QueryParam("topK") Integer topK,
    		@QueryParam("fixedProperty") String fixedProperty,
    		@QueryParam("language") String language,
    		@QueryParam("maxHops") Integer maxHops,
    		@HeaderParam("Accept") String outputMime)
	{
		
		RDFFormat outputFormat = Rio.getParserFormatForMIMEType(outputMime.split(",")[0]);
		if (outputFormat == null) {
			outputFormat = RDFFormat.TURTLE;
		}
		String [] fixedProperties = new String[0];
		if (fixedProperty != null) {
			fixedProperties = fixedProperty.split(",");
		}
		Response r = executeQuery(entity, topK, maxHops, fixedProperties, language, outputFormat);
		return Response.fromResponse(r).status(200).header("Location", null).build();

	}
	
	private Response executeQuery(String entity, Integer topK, Integer maxHops,
			String [] fixedProperties, String language, RDFFormat outputFormat) {
		ValueFactory f = ValueFactoryImpl.getInstance();
		String mime = outputFormat.getMIMETypes().get(0);
		
		entity = filter(entity);
    	language = filter(language);
    	for (String string : fixedProperties) {
			string = filter(string);
		}
    	
    	List<TripleMeta> res = null;
    	java.net.URI uri = null;
		try {
			uri = new java.net.URI(entity);
			res = new SimpleSummarizer().summarize(uri, fixedProperties, topK, maxHops, language);
			
		} catch (NullPointerException e){
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		Model result = createModel(entity, topK, maxHops, fixedProperties, language, res);
		String r = result.filter(null, RDF.TYPE,f.createURI(SUMMARY)).subjects().iterator().next().stringValue();
		StringWriter writer = new StringWriter();
		try {
			Rio.write(result, writer, outputFormat);
			String s = writer.toString();
			return Response.created(new java.net.URI(r)).header("Content-Type", mime).header("Access-Control-Allow-Origin", "*").entity(s).build();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;
	}
	
    private Model createModel(String entity, Integer topK, Integer maxHops, 
    		String [] fixedProperty, String language, List<TripleMeta> meta) {
    	Model model = new LinkedHashModel(); 
    	ValueFactory f = ValueFactoryImpl.getInstance();
    	String uri = PATH + "?entity=" + entity + "&topK=" + topK;
    	if (maxHops != null) {
    		uri += "&maxHops=" + maxHops;
    	}
    	if (language != null) {
    		 uri += "&language=" + language;
    	}
    	if (fixedProperty.length > 0) {
    		 uri += "&fixedProperty=";
    	}
    	for (String property : fixedProperty) {
			uri += property + ",";
		}
    	if (uri.endsWith(",")) {
    		uri = uri.substring(0, uri.length() - 1);	
    	}    	
    	
    	Resource r = null;
		r = f.createURI(uri);
    	model.add(f.createStatement(r, RDF.TYPE, f.createURI(SUMMARY)));
    	model.add(f.createStatement(r, f.createURI(ENTITY), f.createURI(entity)));
    	model.add(f.createStatement(r, f.createURI(TOP_K), f.createLiteral(topK)));
    	if (maxHops != null) {
    		model.add(f.createStatement(r, f.createURI(MAX_HOPS), f.createLiteral(maxHops)));	
    	}
    	if (language != null) {
    		model.add(f.createStatement(r, f.createURI(LANGUAGE), f.createLiteral(language)));	
    	}    	
    	for (String property : fixedProperty) {
    		model.add(f.createStatement(r, f.createURI(FIXED_PROPERTY), f.createURI(property)));
		}
    	for (TripleMeta triple : meta) {
    		Resource stmt = f.createBNode();
    		model.add(f.createStatement(stmt, RDF.TYPE, RDF.STATEMENT));
    		model.add(f.createStatement(stmt, RDF.SUBJECT, f.createURI(triple.getSubject().getURI().toString())));
    		model.add(f.createStatement(stmt, RDF.PREDICATE, f.createURI(triple.getPredicate().getURI().toString())));
    		model.add(f.createStatement(stmt, RDF.OBJECT, f.createURI(((URI) triple.getObject()).getURI().toString())));
    		
    		// add labels
    		String subjectLabel = triple.getSubject().getLabel();
    		String predicateLabel = triple.getPredicate().getLabel();
    		String objectLabel = ((URI) triple.getObject()).getLabel();
    		if (subjectLabel != null) {
    			model.add(f.createStatement(f.createURI(triple.getSubject().getURI().toString()), RDFS.LABEL, f.createLiteral(subjectLabel, "en")));	
    		}
    		if (predicateLabel != null) {
    			model.add(f.createStatement(f.createURI(triple.getPredicate().getURI().toString()), RDFS.LABEL, f.createLiteral(predicateLabel, "en")));	
    		}    		
    		if (objectLabel != null) {
        		model.add(f.createStatement(f.createURI(((URI) triple.getObject()).getURI().toString()), RDFS.LABEL, f.createLiteral(objectLabel, "en")));	
    		}
    		
        	model.add(f.createStatement(r, f.createURI(STATEMENT), stmt));
        	Resource rank = f.createBNode();
        	model.add(f.createStatement(stmt, f.createURI(HAS_RANK), rank));
        	model.add(f.createStatement(rank, f.createURI(RANK_VALUE), f.createLiteral(triple.getRank())));
        	model.add(f.createStatement(f.createURI(uri+"#id"), OWL.SAMEAS, f.createURI(entity)));
		}
    	
    	return model;

    }
    
    private String filter(String string) {
    	if (string == null) {
    		return string;
    	}
    	if (string.startsWith("\"") && string.endsWith("\"")) {
    		string = string.substring(1, string.length() - 1);
    	}
    	if (string.startsWith("\'") && string.endsWith("\'")) {
    		string = string.substring(1, string.length() - 1);
    	}
    	if (string.startsWith("<") && string.endsWith(">")) {
    		string = string.substring(1, string.length() - 1);
    	}
    	return string;
    }
}
