package ebrealtimestreamprocessing.java;

import java.io.Serializable;

/**
 * @Description 说明类的用途
 * @ClassName AdProvinceTopN
 * @Author zzd
 * @Date 2019/10/9 20:42
 * @Version 1.0
 **/
public class AdProvinceTopN implements Serializable {
    private String timestamp;

    private String adId;

    private String province;

    private Long clickedCount;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getClickedCount() {
        return clickedCount;
    }

    public void setClickedCount(Long clickedCount) {
        this.clickedCount = clickedCount;
    }
}
