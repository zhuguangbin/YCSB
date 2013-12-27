package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;

public class CustomKeyWorkload extends CoreWorkload {

  public static final String CUSTOMKEYS_FILE_PROPERTY = "customkeysfile";

  private List<String> keys = new ArrayList<String>();

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    
    // build key from file
    String customkeys = p.getProperty(CUSTOMKEYS_FILE_PROPERTY);
    System.err.println("Loading keys from file : " + customkeys);
    
    initKeysFromFile(customkeys);
    // shuffle for random
    Collections.shuffle(keys);
  }

  private void initKeysFromFile(String customkeysFilePath) {
    InputStream fis;
    BufferedReader br = null;
    String line;
    try {
      fis = new FileInputStream(customkeysFilePath);
      br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
      while ((line = br.readLine()) != null) {
        keys.add(line);
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // Done with the file
      try {
        br.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      br = null;
      fis = null;
    }
  }

  @Override
  public String buildKeyName(long keynum) {
    
    if (!orderedinserts)
    {
      keynum=Utils.hash(keynum);
    }
    int keyindex = (int)keynum % (keys.size());
    
    return keys.get(keyindex);
  }

}
