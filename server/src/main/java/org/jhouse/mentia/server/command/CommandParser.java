package org.jhouse.mentia.server.command;

import org.jhouse.mentia.server.ServerConfig;
import org.jhouse.mentia.store.Store;
import org.jhouse.mentia.store.StoreConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class CommandParser {

    private ServerConfig serverConfig;

    /**
     *  byte => 1, 2, 3 => OPEN, GET, PUT
     *  OPEN =>
     *      BYTE => 0     0                0 0 0 0 0 0
     *              |     |                |
     *            async   indexJournal     segmentIndexFoldMark
     *      INT => indexJournalFlushWatermark
     *      INT => segmentIndexFoldMark
     *
     */

    private Command readOpen(InputStream in) throws IOException {
        byte flags = (byte)in.read();
        boolean async = (flags & 64) > 0;
        boolean indexJournalFlushWaterMarkPresent = (flags & 32) > 0;
        boolean segmentIndexFoldMarkPresent = (flags & 16) > 0;
        StoreConfig config = new StoreConfig();
        if(indexJournalFlushWaterMarkPresent) {
            int indexJournalFlushWatermark = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            config.setIndexJournalFlushWatermark(indexJournalFlushWatermark);
        }

        if(segmentIndexFoldMarkPresent) {
            int segmentIndexFoldMark = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            config.setSegmentIndexFoldMark(segmentIndexFoldMark);
        }
        byte nameLen = (byte)in.read();
        String storeName = new String(in.readNBytes(nameLen));

//        var store = Store.open(serverConfig.getServerBaseLocation() + "/" +storeName);
        return null;
    }

    public Command parse(InputStream in) {
        try {
            int type = in.read();

            if(type == CommandType.OPEN.label) {

            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;

    }

}
