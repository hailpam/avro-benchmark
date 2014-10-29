
package org.pm.avro.test;

import example.avro.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author pmaresca
 */
public class AvroSerializer 
{
    private static AvroSerializer instance;
    
    private AvroSerializer() {}
    
    public synchronized static AvroSerializer instance() 
    {
        if(instance == null)
            instance = new AvroSerializer();
        
        return instance;
    }
    
    public byte[] serialize(Message msg) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Message> writer = new SpecificDatumWriter<>(Message.getClassSchema());
        
        writer.write(msg, encoder);
        encoder.flush();
        out.close();
        
        return out.toByteArray();
    }
    
    public Object deserialize(byte[] msg) throws IOException
    { 
        SpecificDatumReader<Message> reader = new SpecificDatumReader<>(Message.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(msg, null);
        
        return reader.read(null, decoder);
    }
    
    public void setUp() throws IOException { /**TODO : reuse*/ }
    
    public void tearDown() throws IOException { /**TODO : reuse*/ }
    
}
