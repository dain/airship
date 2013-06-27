package io.airlift.airship.coordinator;

import com.google.common.util.concurrent.FutureCallback;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpStatus;

import java.net.URI;

public class SimpleHttpResponseHandler<T>
        implements FutureCallback<JsonResponse<T>>
{
    private final SimpleHttpResponseCallback<T> callback;

    private final URI uri;

    public SimpleHttpResponseHandler(SimpleHttpResponseCallback<T> callback, URI uri)
    {
        this.callback = callback;
        this.uri = uri;
    }

    @Override
    public void onSuccess(JsonResponse<T> response)
    {
        try {
            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                callback.success(response.getValue());
            }
            else {
                // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                Exception cause = response.getException();
                if (cause == null) {
                    cause = new RuntimeException(String.format("Expected response code from %s to be %s, but was %s: %s",
                            uri,
                            HttpStatus.OK.code(),
                            response.getStatusCode(),
                            response.getStatusMessage()));
                }
                callback.fatal(cause);
            }
        }
        catch (Throwable t) {
            // this should never happen
            callback.fatal(t);
        }
    }

    @Override
    public void onFailure(Throwable t)
    {
        callback.fatal(t);
    }

    public interface SimpleHttpResponseCallback<T>
    {
        void success(T value);

        void fatal(Throwable cause);
    }
}
