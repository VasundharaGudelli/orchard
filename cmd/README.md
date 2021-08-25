# CMD

All application and service code entrypoints are put in their own subdirectory inside cmd. In most cases you will simply have a `server` subdirectory that sets up a server running your actual code inside of `pkg` and `internal`.

You may also have subdirectories for different applications of this codebase, such as `cli` for a terminal based tool using the same codebase.
