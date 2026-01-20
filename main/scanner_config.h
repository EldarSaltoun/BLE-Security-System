#ifndef SCANNER_CONFIG_H
#define SCANNER_CONFIG_H

// ==========================================
// SCANNER IDENTITY CONFIGURATION
// ==========================================
// Change this ID (1-4) before flashing each board.
// ID 1 = (0,0)  |  ID 2 = (X,0)
// ID 3 = (0,Y)  |  ID 4 = (X,Y)
#define SCANNER_ID  1

// ==========================================
// GRID CONFIGURATION (Optional for now)
// ==========================================
// Coordinates in meters (if you want to send them directly)
#if SCANNER_ID == 1
    #define SCANNER_POS_X  0.0
    #define SCANNER_POS_Y  0.0
#elif SCANNER_ID == 2
    #define SCANNER_POS_X  5.0  // Example width
    #define SCANNER_POS_Y  0.0
#elif SCANNER_ID == 3
    #define SCANNER_POS_X  0.0
    #define SCANNER_POS_Y  5.0  // Example depth
#elif SCANNER_ID == 4
    #define SCANNER_POS_X  5.0
    #define SCANNER_POS_Y  5.0
#endif

#endif // SCANNER_CONFIG_H