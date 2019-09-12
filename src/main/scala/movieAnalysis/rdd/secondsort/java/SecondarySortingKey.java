package movieAnalysis.rdd.secondsort.java;

import scala.Serializable;
import scala.math.Ordered;

/**
 * key值类，用以比较的类
 * @Description 说明类的用途
 * @ClassName SecondarySortingKey
 * @Author zzd
 * @Create 2019/9/12 13:57
 * @Version 1.0
 **/
public class SecondarySortingKey implements Ordered<SecondarySortingKey>, Serializable {
    private int first;
    private int second;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SecondarySortingKey that = (SecondarySortingKey) obj;
        if (first != that.getFirst()) return false;
        return second == that.getSecond();
    }


    @Override
    public int compare(SecondarySortingKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortingKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater(SecondarySortingKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $less$eq(SecondarySortingKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater$eq(SecondarySortingKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(SecondarySortingKey that) {
        if(this.first-that.getFirst()!=0){
            return this.first-that.getFirst();
        }else {
            return this.second-that.getSecond();
        }
    }

    public SecondarySortingKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
