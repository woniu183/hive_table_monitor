package Test;


import static jodd.util.MathUtil.isEven;

public class Demo {

    public static void main(String[] args){
        //int[] a={1,-3,-5,1,4,-5,6};

        //System.out.println(maxSubSum(a));
        System.out.println(pow(10,10));





    }

    public static int maxSubSum(int[] a){
        int maxSum=0,thisSum=0;
        for(int j=0;j<a.length;j++){
            thisSum +=a[j];
            if(thisSum>maxSum) maxSum=thisSum;
            else if(thisSum<0) thisSum=0;

        }
        return maxSum;

    }

    public static long pow(long x,int n){
        if(n==0) return 1;
        if(n==1) return x;
        if(isEven(n)) return pow(x*x,n/2);
        else     return pow(x*x,n/2)*x;
    }

}
