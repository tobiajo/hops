package old_io.hops.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.Map;

public class Utils {
  
  public static void addFileToLocalResources(
      Map<String, LocalResource> localResources, Configuration yarnConf,
      ApplicationId appId, String fileSrcPath, String fileDstPath)
      throws IOException {
    FileSystem fs = FileSystem.get(yarnConf);
    Path dst = new Path(fs.getHomeDirectory(),
        buildPath(".yarntf", appId.toString(), fileDstPath));
    fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    
    FileStatus fileStatus = fs.getFileStatus(dst);
    
    URL url = ConverterUtils.getYarnUrlFromPath(dst);
    LocalResourceType type = LocalResourceType.FILE;
    LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    long size = fileStatus.getLen();
    long timestamp = fileStatus.getModificationTime();
    
    localResources.put(fileDstPath,
        LocalResource.newInstance(url, type, visibility, size, timestamp));
  }
  
  public static void addClasspathToEnvironment(Map<String, String> environment,
      Configuration yarnConf) {
    StringBuilder classPathEnv =
        new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
            .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    
    if (yarnConf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    
    environment.put("CLASSPATH", classPathEnv.toString());
  }
  
  private static String buildPath(String... components) {
    return StringUtils.join(Path.SEPARATOR, components);
  }
}
