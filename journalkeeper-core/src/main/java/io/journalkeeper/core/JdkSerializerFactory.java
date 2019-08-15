package io.journalkeeper.core;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.exception.SerializeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @author LiYue
 * Date: 2019-08-12
 */
public class JdkSerializerFactory {
   public static  <T extends Serializable> Serializer<T> createSerializer(Class<T> tClass) {
       return new Serializer<T>() {
           @Override
           public byte[] serialize(T entry) {
               try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                   oos.writeObject(entry);
                   return bos.toByteArray();
               } catch (IOException ioe) {
                   throw new SerializeException(ioe);
               }
           }

           @Override
           public T parse(byte[] bytes) {
               if(null == bytes || bytes.length == 0) return null;
               try(ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream ois = new ObjectInputStream(bis)){
                   return tClass.cast(ois.readObject());
               } catch (IOException | ClassNotFoundException ioe) {
                   throw new SerializeException(ioe);
               }
           }
       };
   }
}
