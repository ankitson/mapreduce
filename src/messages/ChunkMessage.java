package messages;

import dfs.Chunk;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 3:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class ChunkMessage extends Message {
    private Chunk chunk;

    public ChunkMessage(Chunk chunk) {
        this.chunk = chunk;
    }

    public Chunk getChunk() {
        return chunk;
    }
}
