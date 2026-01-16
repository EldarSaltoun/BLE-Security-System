%% ============================================================
%  BLE Physical Device Identification (IDENTITY-FIRST)
%  Uses manufacturer signature + service profile
%  + Auto-generated cluster pattern graphs
% ============================================================

clear; clc; close all;

%% ---------------- CONFIG ----------------
jsonFile = 'ble_session_20260105_LivingRoom.json';   % <<< CHANGE IF NEEDED
outDir   = 'cluster_output_livingroom_new_last';

if ~exist(outDir,'dir'), mkdir(outDir); end
patDir = fullfile(outDir,'patterns');
if ~exist(patDir,'dir'), mkdir(patDir); end
plotDir = fullfile(outDir,'plots');
if ~exist(plotDir,'dir'), mkdir(plotDir); end

%% ---------------- LOAD JSON ----------------
raw = jsondecode(fileread(jsonFile));
events = struct2table(raw.events);

%% ---------------- NORMALIZE TIME AXIS ----------------
if ismember('timestamp_esp_us', events.Properties.VariableNames)
    events.time_axis = double(events.timestamp_esp_us) * 1e-6; % seconds
    timeLabel = 'Time since ESP boot (s)';
elseif ismember('timestamp_local', events.Properties.VariableNames)
    events.time_axis = datetime(events.timestamp_local, ...
        'InputFormat','yyyy-MM-dd''T''HH:mm:ss.SSS');
    timeLabel = 'Local Time';
else
    events.time_axis = (1:height(events))';
    timeLabel = 'Sample Index';
end

%% ---------------- NORMALIZE OPTIONAL FIELDS ----------------

% Ensure mfg_sig always exists
if ~ismember('mfg_sig', events.Properties.VariableNames)
    events.mfg_sig = strings(height(events),1);
else
    events.mfg_sig = string(events.mfg_sig);
end

% Ensure mfg_data always exists
if ~ismember('mfg_data', events.Properties.VariableNames)
    events.mfg_data = strings(height(events),1);
end

%% ---------------- NORMALIZE FIELDS ----------------

% MAC
if isstring(events.mac)
    events.mac = cellstr(events.mac);
end

% Manufacturer ID
mfg_id = zeros(height(events),1);
for i = 1:height(events)
    mfg_id(i) = hex2dec(erase(events.mfg(i).raw_hex,'0x'));
end
events.mfg_id = mfg_id;

% Service profile
events.has_services   = double(events.has_services);
events.n_services_16  = double(events.n_services_16);
events.n_services_128 = double(events.n_services_128);

% RSSI
events.rssi = double(events.rssi);

%% ---------------- BUILD ROBUST IDENTITY KEY ----------------

identity_key = strings(height(events),1);

for i = 1:height(events)

    % Case 1: Manufacturer signature (strongest)
    if strlength(events.mfg_sig(i)) > 0 && events.mfg_sig(i) ~= "NONE"

        identity_key(i) = "MFGSIG_" + ...
            string(events.mfg_id(i)) + "_" + events.mfg_sig(i);

    % Case 2: Service-based identity
    elseif events.has_services(i) == 1

        identity_key(i) = "SRV_" + ...
            string(events.mfg_id(i)) + "_" + ...
            string(events.n_services_16(i)) + "_" + ...
            string(events.n_services_128(i));

    % Case 3: Manufacturer only (fallback)
    else
        identity_key(i) = "MFGONLY_" + string(events.mfg_id(i));
    end
end

events.identity_key = identity_key;

%% ---------------- ASSIGN PHYSICAL DEVICE IDS ----------------
[physID, physKey] = findgroups(events.identity_key);
events.physical_id = physID;

fprintf("Detected physical devices: %d\n", numel(physKey));

%% ---------------- PER-PHYSICAL-DEVICE SUMMARY ----------------
physTable = table();

for p = unique(physID)'
    idx = physID == p;
    E = events(idx,:);

    row = table( ...
        p, ...
        height(unique(E.mac)), ...
        mode(E.mfg_id), ...
        unique(E.mfg_sig), ...
        mean(E.rssi), ...
        std(E.rssi), ...
        'VariableNames', { ...
            'physical_id','n_macs','mfg_id','mfg_sig', ...
            'rssi_mean','rssi_std' ...
        });

    physTable = [physTable; row];

    % Save per-device pattern
    writetable(E, fullfile(patDir, ...
        sprintf('physical_device_%d.csv', p)));
end

%% ---------------- SAVE OUTPUT TABLES ----------------
writetable(events, fullfile(outDir,'events_with_physical_id.csv'));
writetable(physTable, fullfile(outDir,'physical_devices.csv'));

disp(physTable);

%% ================= CLUSTER PATTERN PLOTS =================
fprintf('Generating cluster pattern plots...\n');

for p = unique(events.physical_id)'

    idx = events.physical_id == p;
    E   = events(idx,:);

    % Skip very small clusters
    if height(E) < 5
        continue;
    end

    %% ---- SORT BY TIME ----
    [t, order] = sort(E.time_axis);
    rssi = E.rssi(order);

    %% ---- MEAN RSSI (smoothed) ----
    win = max(3, round(numel(rssi) * 0.05)); % adaptive smoothing window
    rssi_mean = movmean(rssi, win);

    %% ---- ADVERTISING INTERVAL ----
    adv_int = diff(t);              % seconds
    adv_int = adv_int(adv_int > 0); % sanity
    adv_mean = movmean(adv_int, win);

    %% ================= PLOT =================
    fig = figure('Visible','off','Color','w','Position',[100 100 900 600]);

    tiledlayout(2,1,'Padding','compact','TileSpacing','compact');

    % ---- RSSI Pattern ----
    nexttile;
    plot(t, rssi_mean, 'LineWidth',1.5);
    grid on;
    ylabel('RSSI Mean (dBm)');
    title(sprintf('Physical Device %d – RSSI Pattern', p));

    % ---- Advertising Interval Pattern ----
    nexttile;
    plot(t(2:end), adv_mean, 'LineWidth',1.5);
    grid on;
    ylabel('Advertising Interval (s)');
    xlabel(timeLabel);
    title('Advertising Interval Pattern');

    %% ---- SAVE FIGURE ----
    fname = fullfile(plotDir, sprintf('cluster_%d_patterns.png', p));
    exportgraphics(fig, fname, 'Resolution',200);
    close(fig);

end

fprintf('Cluster pattern plots saved in: %s\n', plotDir);
