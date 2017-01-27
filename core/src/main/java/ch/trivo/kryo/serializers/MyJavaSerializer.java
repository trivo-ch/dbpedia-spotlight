//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package ch.trivo.kryo.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class MyJavaSerializer extends Serializer<Object> {
    public MyJavaSerializer() {
    }

    public void write(Kryo kryo, Output output, Object object) {
        try {
            ObjectMap ex = kryo.getGraphContext();
            ObjectOutputStream objectStream = (ObjectOutputStream)ex.get(this);
            if(objectStream == null) {
                objectStream = new ObjectOutputStream(output);
                ex.put(this, objectStream);
            }

            objectStream.writeObject(object);
            objectStream.flush();
        } catch (Exception var6) {
            throw new KryoException("Error during Java serialization.", var6);
        }
    }

    public Object read(Kryo kryo, Input input, Class type) {
        try {
            System.out.println(kryo.getClassLoader());
            ObjectMap ex = kryo.getGraphContext();
            ObjectInputStream objectStream = (ObjectInputStream)ex.get(this);
            if(objectStream == null) {
                objectStream = new ObjectInputStream(input);
                ex.put(this, objectStream);
            }

            return objectStream.readObject();
        } catch (Exception var6) {
            throw new KryoException("Error during Java deserialization.", var6);
        }
    }
}
