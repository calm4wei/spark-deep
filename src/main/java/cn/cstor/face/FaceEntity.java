package cn.cstor.face;

/**
 * Created on 2016/12/7
 *
 * @author feng.wei
 */
public class FaceEntity implements Comparable<FaceEntity> {

    private String address;
    private Float[] floats;
    private Double rate;

    public FaceEntity() {
    }

    public FaceEntity(String address, Float[] floats, Double rate) {
        this.address = address;
        this.floats = floats;
        this.rate = rate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Float[] getFloats() {
        return floats;
    }

    public void setFloats(Float[] floats) {
        this.floats = floats;
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    @Override
    public int compareTo(FaceEntity o) {
        if (this.rate > o.getRate()) {
            return 1;
        } else if (this.rate < o.getRate()) {
            return -1;
        } else {
            return 0;
        }
    }
}
