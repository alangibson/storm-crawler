package com.digitalpebble.stormcrawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.parse.Outlink;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.digitalpebble.stormcrawler.util.URLUtil;

public abstract class StatusEmitterBase {

    private URLFilters urlFilters;

    private MetadataTransfer metadataTransfer;

    private boolean allowRedirs;

    protected CallbackCollector collector;

    public void prepare(Map stormConf, IMetricsContext context, CallbackCollector collector) {
        this.collector = collector;
        urlFilters = URLFilters.fromConf(stormConf);
        metadataTransfer = MetadataTransfer.getInstance(stormConf);
        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.stormcrawler.Constants.AllowRedirParamName,
                true);
    }

    /** Used for redirections or when discovering sitemap URLs **/
    protected void emitOutlink(TupleWrapper t, URL sURL, String newUrl,
            Metadata sourceMetadata, String... customKeyVals) {

        Outlink ol = filterOutlink(sURL, newUrl, sourceMetadata, customKeyVals);
        if (ol == null)
            return;

        collector.emit(
                com.digitalpebble.stormcrawler.Constants.StatusStreamName, t,
                new Values(ol.getTargetURL(), ol.getMetadata(),
                        Status.DISCOVERED));
    }

    protected Outlink filterOutlink(URL sURL, String newUrl,
            Metadata sourceMetadata, String... customKeyVals) {
        // build an absolute URL
        try {
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            return null;
        }

        // apply URL filters
        newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);

        // filtered
        if (newUrl == null) {
            return null;
        }

        Metadata metadata = metadataTransfer.getMetaForOutlink(newUrl,
                sURL.toExternalForm(), sourceMetadata);

        for (int i = 0; i < customKeyVals.length; i = i + 2) {
            metadata.addValue(customKeyVals[i], customKeyVals[i + 1]);
        }

        Outlink l = new Outlink(newUrl);
        l.setMetadata(metadata);
        return l;
    }

    protected boolean allowRedirs() {
        return allowRedirs;
    }

}
