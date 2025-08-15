package com.sephora.services.ismsearchpoc.search;

import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class PageInfo {
    private boolean paginate;
    private PaginationMode mode;
    private Integer page;       // OFFSET
    private Integer size;
    private Integer returned;
    private Long total;         // may be null if includeTotal=false or unavailable
    private String cursor;      // inbound cursor
    private String nextCursor;  // KEYSET next
    private boolean hasMore;
}
