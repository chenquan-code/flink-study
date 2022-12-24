package com.cq.flink.table.demo01;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author chenquan
 * @Description
 * @Date 2022-04-29 1:47
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordAndOne {

    public String word;
    public int wordCount;

}
