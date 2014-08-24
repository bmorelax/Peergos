package peergos.user.fs;

import peergos.crypto.SymmetricKey;
import peergos.crypto.UserPublicKey;
import peergos.user.UserContext;
import peergos.util.ByteArrayWrapper;
import peergos.util.Serialize;

import java.io.*;

public class Location
{
    public final String owner;
    public final UserPublicKey subKey;
    public final ByteArrayWrapper mapKey;

    public Location(String owner, UserPublicKey subKey, ByteArrayWrapper mapKey) {
        this.owner = owner;
        this. subKey = subKey;
        this.mapKey = mapKey;
    }

    public byte[] encrypt(SymmetricKey key, byte[] iv) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            DataOutputStream dout = new DataOutputStream(bout);
            Serialize.serialize(owner, dout);
            Serialize.serialize(subKey.getPublicKey(), dout);
            Serialize.serialize(mapKey.data, dout);
        } catch (IOException e) {e.printStackTrace();}
        return key.encrypt(bout.toByteArray(), iv);
    }

    public static Location decrypt(SymmetricKey key, byte[] iv, byte[] data) throws IOException {
        byte[] raw = key.decrypt(data, iv);
        ByteArrayInputStream bin = new ByteArrayInputStream(raw);
        DataInputStream din = new DataInputStream(bin);
        String owner = Serialize.deserializeString(din, UserContext.MAX_USERNAME_SIZE);
        UserPublicKey pub = new UserPublicKey(Serialize.deserializeByteArray(din, UserPublicKey.RSA_KEY_SIZE/8));
        ByteArrayWrapper mapKey = new ByteArrayWrapper(Serialize.deserializeByteArray(din, UserPublicKey.HASH_SIZE));
        return new Location(owner, pub, mapKey);
    }
}