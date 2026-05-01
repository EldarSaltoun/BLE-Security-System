#pragma once

/*
 * scanner_config.h
 *
 * Per-board scanner identity.
 *
 * IMPORTANT:
 *   Before flashing each ESP32 scanner, set SCANNER_ID to a unique value:
 *     1, 2, 3, or 4
 *
 * The PC receiver stores scanner IDs as strings, so numeric IDs are fine.
 * The JSON sent by the ESP32 will contain:
 *     "scanner": <SCANNER_ID>
 *
 * Suggested physical placement mapping, matching your room/trapezoid workflow:
 *   ID 1 = Scanner 1
 *   ID 2 = Scanner 2
 *   ID 3 = Scanner 3
 *   ID 4 = Scanner 4
 */

/* Change this manually before flashing each board. */
#define SCANNER_ID  1

/*
 * Scanner coordinates.
 *
 * These are optional metadata for future use. The current ESP32 JSON does not
 * send these coordinates; MATLAB/Python localization should continue using the
 * calibration CSV / room configuration as the source of truth.
 *
 * Units: meters.
 *
 * Keep these values aligned with your room coordinate system if you later decide
 * to transmit scanner positions from firmware.
 */
#if SCANNER_ID == 1
    #define SCANNER_POS_X_M  0.0f
    #define SCANNER_POS_Y_M  0.0f
    #define SCANNER_POS_Z_M  0.0f
#elif SCANNER_ID == 2
    #define SCANNER_POS_X_M  0.0f
    #define SCANNER_POS_Y_M  5.0f
    #define SCANNER_POS_Z_M  0.0f
#elif SCANNER_ID == 3
    #define SCANNER_POS_X_M  5.0f
    #define SCANNER_POS_Y_M  5.0f
    #define SCANNER_POS_Z_M  0.0f
#elif SCANNER_ID == 4
    #define SCANNER_POS_X_M  5.0f
    #define SCANNER_POS_Y_M  0.0f
    #define SCANNER_POS_Z_M  0.0f
#else
    #error "Invalid SCANNER_ID. Set SCANNER_ID to 1, 2, 3, or 4."
#endif

/*
 * Backwards-compatible aliases.
 * Some older code may still reference SCANNER_POS_X / SCANNER_POS_Y.
 */
#define SCANNER_POS_X  SCANNER_POS_X_M
#define SCANNER_POS_Y  SCANNER_POS_Y_M
#define SCANNER_POS_Z  SCANNER_POS_Z_M
