//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.numaproj.confluent.kafka_sink;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.data.RecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.*;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

@Setter
@Getter
@AvroGenerated
public class Payment extends SpecificRecordBase implements SpecificRecord {
    private static final long serialVersionUID = -3932533050902953675L;
    public static final Schema SCHEMA$ = (new Schema.Parser()).parse("{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}");
    private static final SpecificData MODEL$ = new SpecificData();
    private static final BinaryMessageEncoder<Payment> ENCODER;
    private static final BinaryMessageDecoder<Payment> DECODER;
    private CharSequence id;
    private double amount;
    private static final DatumWriter<Payment> WRITER$;
    private static final DatumReader<Payment> READER$;

    public static Schema getClassSchema() {
        return SCHEMA$;
    }

    public static BinaryMessageEncoder<Payment> getEncoder() {
        return ENCODER;
    }

    public static BinaryMessageDecoder<Payment> getDecoder() {
        return DECODER;
    }

    public static BinaryMessageDecoder<Payment> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder(MODEL$, SCHEMA$, resolver);
    }

    public ByteBuffer toByteBuffer() throws IOException {
        return ENCODER.encode(this);
    }

    public static Payment fromByteBuffer(ByteBuffer b) throws IOException {
        return (Payment)DECODER.decode(b);
    }

    public Payment() {
    }

    public Payment(CharSequence id, Double amount) {
        this.id = id;
        this.amount = amount;
    }

    public SpecificData getSpecificData() {
        return MODEL$;
    }

    public Schema getSchema() {
        return SCHEMA$;
    }

    public Object get(int field$) {
        switch (field$) {
            case 0:
                return this.id;
            case 1:
                return this.amount;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    public void put(int field$, Object value$) {
        switch (field$) {
            case 0:
                this.id = (CharSequence)value$;
                break;
            case 1:
                this.amount = (Double)value$;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Builder other) {
        return other == null ? new Builder() : new Builder(other);
    }

    public static Builder newBuilder(Payment other) {
        return other == null ? new Builder() : new Builder(other);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    public void readExternal(ObjectInput in) throws IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

    protected boolean hasCustomCoders() {
        return true;
    }

    public void customEncode(Encoder out) throws IOException {
        out.writeString(this.id);
        out.writeDouble(this.amount);
    }

    public void customDecode(ResolvingDecoder in) throws IOException {
        Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
        if (fieldOrder == null) {
            this.id = in.readString(this.id instanceof Utf8 ? (Utf8)this.id : null);
            this.amount = in.readDouble();
        } else {
            for(int i = 0; i < 2; ++i) {
                switch (fieldOrder[i].pos()) {
                    case 0:
                        this.id = in.readString(this.id instanceof Utf8 ? (Utf8)this.id : null);
                        break;
                    case 1:
                        this.amount = in.readDouble();
                        break;
                    default:
                        throw new IOException("Corrupt ResolvingDecoder.");
                }
            }
        }

    }

    static {
        ENCODER = new BinaryMessageEncoder(MODEL$, SCHEMA$);
        DECODER = new BinaryMessageDecoder(MODEL$, SCHEMA$);
        WRITER$ = MODEL$.createDatumWriter(SCHEMA$);
        READER$ = MODEL$.createDatumReader(SCHEMA$);
    }

    @AvroGenerated
    public static class Builder extends SpecificRecordBuilderBase<Payment> implements RecordBuilder<Payment> {
        private CharSequence id;
        private double amount;

        private Builder() {
            super(Payment.SCHEMA$, Payment.MODEL$);
        }

        private Builder(Builder other) {
            super(other);
            if (isValidValue(this.fields()[0], other.id)) {
                this.id = (CharSequence)this.data().deepCopy(this.fields()[0].schema(), other.id);
                this.fieldSetFlags()[0] = other.fieldSetFlags()[0];
            }

            if (isValidValue(this.fields()[1], other.amount)) {
                this.amount = (Double)this.data().deepCopy(this.fields()[1].schema(), other.amount);
                this.fieldSetFlags()[1] = other.fieldSetFlags()[1];
            }

        }

        private Builder(Payment other) {
            super(Payment.SCHEMA$, Payment.MODEL$);
            if (isValidValue(this.fields()[0], other.id)) {
                this.id = (CharSequence)this.data().deepCopy(this.fields()[0].schema(), other.id);
                this.fieldSetFlags()[0] = true;
            }

            if (isValidValue(this.fields()[1], other.amount)) {
                this.amount = (Double)this.data().deepCopy(this.fields()[1].schema(), other.amount);
                this.fieldSetFlags()[1] = true;
            }

        }

        public CharSequence getId() {
            return this.id;
        }

        public Builder setId(CharSequence value) {
            this.validate(this.fields()[0], value);
            this.id = value;
            this.fieldSetFlags()[0] = true;
            return this;
        }

        public boolean hasId() {
            return this.fieldSetFlags()[0];
        }

        public Builder clearId() {
            this.id = null;
            this.fieldSetFlags()[0] = false;
            return this;
        }

        public double getAmount() {
            return this.amount;
        }

        public Builder setAmount(double value) {
            this.validate(this.fields()[1], value);
            this.amount = value;
            this.fieldSetFlags()[1] = true;
            return this;
        }

        public boolean hasAmount() {
            return this.fieldSetFlags()[1];
        }

        public Builder clearAmount() {
            this.fieldSetFlags()[1] = false;
            return this;
        }

        public Payment build() {
            try {
                Payment record = new Payment();
                record.id = this.fieldSetFlags()[0] ? this.id : (CharSequence)this.defaultValue(this.fields()[0]);
                record.amount = this.fieldSetFlags()[1] ? this.amount : (Double)this.defaultValue(this.fields()[1]);
                return record;
            } catch (AvroMissingFieldException var2) {
                AvroMissingFieldException e = var2;
                throw e;
            } catch (Exception var3) {
                Exception e = var3;
                throw new AvroRuntimeException(e);
            }
        }
    }
}
