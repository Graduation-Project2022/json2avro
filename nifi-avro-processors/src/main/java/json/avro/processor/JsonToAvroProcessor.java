package json.avro.processor;

import com.cdr.CDR;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class JsonToAvroProcessor extends AbstractProcessor {

    private FlowFile flowFile;


    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Success")
            .description("Success")
            .build();

    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder()
            .name("Failure")
            .description("Failure")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();
        String fileName = flowFile.getAttribute("filename");
        session.putAttribute(flowFile, "filename", fileName.substring(0, fileName.lastIndexOf('.')) + "_" + timestamp + ".avro");

        flowFile = session.write(flowFile, (inputStream, outputStream) -> {
            final DatumWriter<CDR> datumWriter = new SpecificDatumWriter<>(CDR.class);
            try (DataFileWriter<CDR> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                CDR.Builder cdrBuilder = CDR.newBuilder();
                CDR cdr = new CDR();

                JsonReader reader = new JsonReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                reader.setLenient(true);
                Gson gson = new Gson();
                reader.beginArray();
                dataFileWriter.create(cdr.getSchema(), outputStream);
                while (reader.hasNext()) {
                    Model model = gson.fromJson(reader, Model.class);
                    cdrBuilder.setMSISDNA(model.getNumberA());
                    cdrBuilder.setMSISDNB(model.getNumberB());
                    cdrBuilder.setCallTime(model.getTimestamp());
                    cdrBuilder.setDuration(model.getDuration());
                    if(model.getNumberA().substring(0,3).equals(model.getNumberB().substring(0,3))){
                        cdrBuilder.setServiceName("sameOperatorCall");
                    }else {
                        cdrBuilder.setServiceName("localCall");
                    }

                    cdr = cdrBuilder.build();
                    dataFileWriter.append(cdr);
                }
                reader.endArray();
                reader.close();
            } catch (IOException ex) {
                logger.error("\nPROCESSOR ERROR 1: " + ex.getMessage() + "\n");
                session.transfer(flowFile, ERROR_RELATIONSHIP);
                return;
            }

            logger.info("Successfully transfer file :)");
        });
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
