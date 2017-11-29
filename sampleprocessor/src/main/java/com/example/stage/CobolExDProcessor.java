package com.example.stage;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "CobolExProcessor",
    description = "",
    icon = "default.png",
    onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class CobolExDProcessor extends CobolExProcessor {
  
  @ConfigDef(
	      required = true,
	      type = ConfigDef.Type.STRING,
	      defaultValue = "/tmp",
	      label = "Cobol CopyBook Directory",
	      displayPosition = 10,
	      group = "FILES"
	  )
	  public String cobolDirectory;

	  /** {@inheritDoc} */
	  @Override
	  public String getCobolDirectory() {
	    return cobolDirectory;
	  }

}