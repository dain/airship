package io.airlift.airship.coordinator;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.CountingInputStream;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class TestValidatingResponseHandler
{
    private static Response fakeJsonResponse(String json)
    {
        InputStream input = new ByteArrayInputStream(json.getBytes(Charsets.UTF_8));
        final CountingInputStream countingInputStream = new CountingInputStream(input);
        return new Response()
        {
            @Override
            public int getStatusCode()
            {
                return HttpStatus.OK.code();
            }

            @Override
            public String getStatusMessage()
            {
                return HttpStatus.OK.reason();
            }

            @Override
            public String getHeader(String name)
            {
                List<String> list = getHeaders().get(name);
                return list.isEmpty() ? null : list.get(0);
            }

            @Override
            public ListMultimap<String, String> getHeaders()
            {
                return ImmutableListMultimap.<String, String>builder()
                        .put(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                        .build();
            }

            @Override
            public long getBytesRead()
            {
                return countingInputStream.getCount();
            }

            @Override
            public InputStream getInputStream()
                    throws IOException
            {
                return countingInputStream;
            }
        };
    }
}
