package com.example.stage;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.LineProvider;
import net.sf.JRecord.Details.RecordDetail;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;

public abstract class CobolExProcessor extends SingleLaneRecordProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(CobolExProcessor.class);
	private CopybookLoader copybookInt = new CobolCopybookLoader();

	/**
	 * Gets the Cobol Data file.
	 * 
	 * @return
	 */
	public abstract String getCobolDirectory();

	/** {@inheritDoc} */
	@Override
	public void destroy() {
		// Clean up any open resources.
		super.destroy();
	}

	/** {@inheritDoc} */
	@Override
	protected List<ConfigIssue> init() {
		LOG.info("Initialization started");
		// Validate configuration values and open any required resources.
		List<ConfigIssue> issues = super.init();

		if (getCobolDirectory().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(Groups.FILES.name(), "config", Errors.ERCODE1,
					"Here's what's wrong..."));
		}
		LOG.info("Initialized successfully");

		// If issues is not empty, the UI will inform the user of each
		// configuration issue in the list.
		return issues;
	}

	/** {@inheritDoc} */
	@Override
	protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
		FileRef fileRef = record.get("/fileRef").getValueAsFileRef();
		record.delete("/fileRef");

		InputStream inputstream = null;
		AbstractLine line = null;
		AbstractLineReader reader = null;

		try {
			inputstream = fileRef.createInputStream(getContext(), InputStream.class);

			LayoutDetail copyBook =	copybookInt.loadCopyBook(
							getCobolDirectory(),
							CopybookLoader.SPLIT_NONE, 0, "cp037",
							CommonBits.getDefaultCobolTextFormat(),
							Convert.FMT_MAINFRAME, 0, null
					).setFileStructure(Constants.IO_FIXED_LENGTH)
							.asLayoutDetail();

			LineProvider provider = LineIOProvider.getInstance().getLineProvider(Constants.IO_FIXED_LENGTH, "cp037");
			reader = LineIOProvider.getInstance().getLineReader(copyBook, provider);
			reader.open(inputstream, copyBook);
			LayoutDetail layout = reader.getLayout();
			RecordDetail rdt = layout.getRecord(0);

			while ((line = reader.read()) != null) {
				for (int j = 0; j < rdt.getFieldCount(); j++) {
					Field field = Field.create(line.getFieldValue(rdt.getField(j).getName()).asString());
					record.set("/" + rdt.getField(j).getName(), field);
				}
				batchMaker.addRecord(record);
			}
		} catch (Exception e) {
			throw new com.streamsets.pipeline.api.base.OnRecordErrorException(record, Errors.ERCODE1, e);
		}
		try {
			reader.close();
		} catch (IOException e) {

			e.printStackTrace();

		}
	}
}