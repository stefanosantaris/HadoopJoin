package HadoopJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 *The TextPair class which provides a way of storing and custom sorting and grouping of two org.apache.hadoop.io.Text objects.
*/
public class TextPair implements WritableComparable<TextPair>{
    
    /**
     *The TextPair internal values.
    */
    private Text firstValue,
                 secondValue;
    
    /**
     *Instantiates a new TextPair object.
    */
    public TextPair(){
        setValues("",
                  "");
    }
    
    /**
     *Instantiates a new TextPair object.
     *
     *@param firstValue The first TextPair internal value.
     *@param secondValue The second TextPair internal value.
    */
    public TextPair(String firstValue,
                    String secondValue){
        setValues(firstValue,
                  secondValue);
    }
    
    /**
     *Sets the TextPair internal values.
     *
     *@param firstValue The first TextPair internal value.
     *@param secondValue The second TextPair internal value.
    */
    private void setValues(String firstValue,
                           String secondValue){
        this.firstValue=new Text(firstValue);
        this.secondValue=new Text(secondValue);
    }
    
    /**
     *Gets the first TextPair internal value.
     *
     *@return The first TextPair internal value.
    */
    public Text getFirstValue(){
        return(firstValue);
    }
    
    /**
     *Gets the second TextPair internal value.
     *
     *@return The second TextPair internal value.
    */
    public Text getSecondValue(){
        return(secondValue);
    }

    /**
     *@see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
    */
    @Override
    public void readFields(DataInput arg0)
                          throws IOException{
        firstValue.readFields(arg0);
        secondValue.readFields(arg0);
        
    }

    /**
     *@see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
    */
    @Override
    public void write(DataOutput arg0)
                     throws IOException{
        firstValue.write(arg0);
        secondValue.write(arg0);
        
    }

    /**
     *@see java.lang.Comparable#compareTo(java.lang.Object)
    */
    @Override
    public int compareTo(TextPair arg0){
        int firstValueComparison=getFirstValue().compareTo(arg0.getFirstValue());
        if(firstValueComparison!=0){
            return(firstValueComparison);
        }
        return(getSecondValue().compareTo(arg0.getSecondValue()));
    }
    
    /**
     *@see java.lang.Object#equals(java.lang.Object)
    */
    @Override
    public boolean equals(Object arg0){
        if(arg0 instanceof TextPair){
            TextPair tp=(TextPair)arg0;
            return(tp.getFirstValue()
                     .equals(getFirstValue()) &&
                   tp.getSecondValue()
                     .equals(getSecondValue()));
        }
        return(false);
    }
   
    /**
     *@see java.lang.Object#hashCode()
    */
    @Override
    public int hashCode(){
        return((getFirstValue().hashCode()*163)+getSecondValue().hashCode());
    }
    
    /**
     *@see java.lang.Object#toString()
    */
    @Override
    public String toString(){
        return(getFirstValue().toString()
                              .concat(" ")
                              .concat(getSecondValue().toString()));
    }
    
    /**
     *The TextPairComparator class which compares two TextPair objects both on their first and their second value.
    */
    public static class TextPairComparator extends WritableComparator{
        
        /**
         *The org.apache.hadoop.io.Text.Comparator used internally by the TextPairComparator.
        */
        private static final Text.Comparator textComparator=new Text.Comparator();
        
        /**
         *Instantiates a new TextPairComparator object using the TextPair class.
        */
        public TextPairComparator(){
            super(TextPair.class);
        }
        
        /**
         *Compares two TextPair objects both on their first and their second value.
         * 
         *@see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
        */
        @Override
        public int compare(byte[] arg0,
                           int arg1,
                           int arg2,
                           byte[] arg3,
                           int arg4,
                           int arg5){
            try{
                int firstL1=WritableUtils.decodeVIntSize(arg0[arg1])+readVInt(arg0,
                                                                              arg1);
                int firstL2=WritableUtils.decodeVIntSize(arg3[arg4])+readVInt(arg3,
                                                                              arg4);
                int cmp=textComparator.compare(arg0,
                                               arg1,
                                               firstL1,
                                               arg3,
                                               arg4,
                                               firstL2);
                if (cmp!=0){
                    return(cmp);
                }
                return(textComparator.compare(arg0,
                                              arg1+firstL1,
                                              arg2-firstL1,
                                              arg3,
                                              arg4+firstL2,
                                              arg5-firstL2));
            }
            catch(IOException ioException){
                throw new IllegalArgumentException(ioException);
            }
        }
    }
    
    /**
     *The TextPairComparator class which compares two TextPair Objects only on their first value.
     */
    public static class TextPairFirstValueComparator extends WritableComparator{
        
        /**
         *The org.apache.hadoop.io.Text.Comparator used internally by the TextPairFirstValueComparator.
        */
        private static final Text.Comparator textComparator=new Text.Comparator();
        
        /**
         *Instantiates a new TextPairFirstValueComparator object using the TextPair class.
        */
        public TextPairFirstValueComparator(){
            super(TextPair.class);
        }
        
        /**
         *Compares two TextPair objects only on their first value.
         *
         *@see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
        */
        @Override
        public int compare(byte[] arg0,
                           int arg1,
                           int arg2,
                           byte[] arg3,
                           int arg4,
                           int arg5){
            try{
                int firstL1=WritableUtils.decodeVIntSize(arg0[arg1])+readVInt(arg0,
                                                                              arg1);
                int firstL2=WritableUtils.decodeVIntSize(arg3[arg4])+readVInt(arg3,
                                                                              arg4);
                return(textComparator.compare(arg0,
                                              arg1,
                                              firstL1,
                                              arg3,
                                              arg4,
                                              firstL2));
            }
            catch(IOException ioException){
                throw new IllegalArgumentException(ioException);
            }
        }
    }
}