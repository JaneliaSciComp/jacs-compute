package org.janelia.jacs2.asyncservice.sample.aux;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

/**
 * Reads the JSON-formatted Zeiss LSM metadata that is output by lsm_json_dump.pl.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LSMMetadata {

    private String stack;
    private Recording recording;
    private List<Channel> channels;
    private List<Track> tracks;
    private List<Laser> lasers;
    private List<Marker> markers;
    private List<Timer> timers;
    
    public List<Channel> getChannels() {
        return channels;
    }

    public List<Laser> getLasers() {
        return lasers;
    }

    public String getStack() {
        return stack;
    }

    public Recording getRecording() {
        return recording;
    }

    public List<Track> getTracks() {
        return tracks;
    }
    
    public List<Track> getNonBleachTracks() {
        List<Track> nonBleach = new ArrayList<Track>();
        for(Track track : tracks) {
            if ("0".equals(track.getIsBleachTrack())) {
                nonBleach.add(track);
            }
        }
        return nonBleach;
    }
    
    public List<Marker> getMarkers() {
        return markers;
    }

    public List<Timer> getTimers() {
        return timers;
    }

    public Track getTrack(Channel channel) {
        String parts[] = channel.getName().split("-");
        if (parts.length<2) {
            List<Track> nonBleach = getNonBleachTracks();
            if (nonBleach.size()>1) {
                throw new IllegalStateException("Channel name ("+channel.getName()+") does not contain track name, and there is more than one non-bleach track ("+tracks.size()+")");
            }
            return nonBleach.get(0);
        }
        String trackId = parts[1];
        for(Track track : tracks) {
            String thisTrackId = track.getName().replaceAll("rack\\s*", "");
            if (trackId.equals(thisTrackId)) {
                return track;
            }
        }
        // Couldn't find the track based on the track names.. let's try multiplex order instead
        for(Track track : tracks) {
            String thisTrackId = "T"+track.getMultiplexOrder();
            if (trackId.equals(thisTrackId)) {
                return track;
            }
        }
        return null;
    }
    
    public DetectionChannel getDetectionChannel(Channel channel) {
        Track track = getTrack(channel);
        if (track==null) return null;
        String parts[] = channel.getName().split("-");
        String chan = parts[0];
        if (track.getDetectionChannels()!=null) {
            for(DetectionChannel detChannel : track.getDetectionChannels()) {
                if (detChannel.getName().equals(chan)) {
                    return detChannel;
                }
            }
        }
        return null;
    }

    public DataChannel getDataChannel(Channel channel) {
        String parts[] = channel.getName().split("-");
        String chan = parts[0];
        Track track = getTrack(channel);
        if (track==null) return null;
        if (track.getDataChannels()!=null) {
            for(DataChannel dataChannel : track.getDataChannels()) {
                if (dataChannel.getName().equals(chan)) {
                    return dataChannel;
                }
            }
        }
        return null;
    }
    
    public static class Channel {
        private String color;
        private String name;
        public String getColor() {
            return color;
        }
        public String getName() {
            return name;
        }
    }
    
    public static class Laser {
        @SerializedName("OLEDB_LASER_ENTRY_ACQUIRE") private String acquire;
        @SerializedName("OLEDB_LASER_ENTRY_NAME") private String name;
        @SerializedName("OLEDB_LASER_ENTRY_POWER") private String power;
        public String getAcquire() {
            return acquire;
        }
        public String getName() {
            return name;
        }
        public String getPower() {
            return power;
        }
    }

    public static class Marker {
        // TODO: implement fields
    }

    public static class Recording {
        // TODO: implement fields
    }
    
    public static class Timer {
        // TODO: implement fields
    }

    public static class Track {
        @SerializedName("TRACK_ENTRY_ACQUIRE") private String acquire;
        @SerializedName("TRACK_ENTRY_BLEACH_COUNT") private String bleachCount;
        @SerializedName("TRACK_ENTRY_BLEACH_SCAN_NUMBER") private String bleachScanNumber;
        @SerializedName("TRACK_ENTRY_IS_BLEACH_AFTER_SCAN_NUMBER") private String isBleachAfterScanNumber;
        @SerializedName("TRACK_ENTRY_IS_BLEACH_TRACK") private String isBleachTrack;
        @SerializedName("TRACK_ENTRY_IS_RATIO_STACK") private String isRatioStack;
        @SerializedName("TRACK_ENTRY_MULTIPLEX_ORDER") private String multiplexOrder;
        @SerializedName("TRACK_ENTRY_MULTIPLEX_TYPE") private String multiplexType;
        @SerializedName("TRACK_ENTRY_NAME") private String name;
        @SerializedName("TRACK_ENTRY_PIXEL_TIME") private String pixelTime;
        @SerializedName("TRACK_ENTRY_SAMPLING_METHOD") private String samplingMethod;
        @SerializedName("TRACK_ENTRY_SAMPLING_MODE") private String samplingMode;
        @SerializedName("TRACK_ENTRY_SAMPLING_NUMBER") private String samplingNumber;
        @SerializedName("TRACK_ENTRY_SPI_CENTER_WAVELENGTH") private String spiCenterWavelength;
        @SerializedName("TRACK_ENTRY_TIME_BETWEEN_STACKS") private String timeBetweenStacks;
        @SerializedName("TRACK_ENTRY_TRIGGER_IN") private String triggerIn;
        @SerializedName("TRACK_ENTRY_TRIGGER_OUT") private String triggerOut;
        @SerializedName("TRACK_LASER_SUPRESSION_MODE") private String laserSupressionMode;
        @SerializedName("TRACK_REFLECTED_LIGHT") private String reflectedLight;
        @SerializedName("TRACK_TRANSMITTED_LIGHT") private String transmittedLight;
        @SerializedName("beam_splitters") private List<BeamSplitter> beamSplitters;
        @SerializedName("data_channels") private List<DataChannel> dataChannels;
        @SerializedName("detection_channels") private List<DetectionChannel> detectionChannels;
        @SerializedName("illumination_channels") private List<IlluminationChannel> illuminationChannels;
        
        public String getAcquire() {
            return acquire;
        }
        public String getBleachCount() {
            return bleachCount;
        }
        public String getBleachScanNumber() {
            return bleachScanNumber;
        }
        public String getIsBleachAfterScanNumber() {
            return isBleachAfterScanNumber;
        }
        public String getIsBleachTrack() {
            return isBleachTrack;
        }
        public String getIsRatioStack() {
            return isRatioStack;
        }
        public String getMultiplexOrder() {
            return multiplexOrder;
        }
        public String getMultiplexType() {
            return multiplexType;
        }
        public String getName() {
            return name;
        }
        public String getPixelTime() {
            return pixelTime;
        }
        public String getSamplingMethod() {
            return samplingMethod;
        }
        public String getSamplingMode() {
            return samplingMode;
        }
        public String getSamplingNumber() {
            return samplingNumber;
        }
        public String getSpiCenterWavelength() {
            return spiCenterWavelength;
        }
        public String getTimeBetweenStacks() {
            return timeBetweenStacks;
        }
        public String getTriggerIn() {
            return triggerIn;
        }
        public String getTriggerOut() {
            return triggerOut;
        }
        public String getLaserSupressionMode() {
            return laserSupressionMode;
        }
        public String getReflectedLight() {
            return reflectedLight;
        }
        public String getTransmittedLight() {
            return transmittedLight;
        }
        public List<BeamSplitter> getBeamSplitters() {
            return beamSplitters;
        }
        public List<DataChannel> getDataChannels() {
            return dataChannels;
        }
        public List<DetectionChannel> getDetectionChannels() {
            return detectionChannels;
        }
        public List<IlluminationChannel> getIlluminationChannels() {
            return illuminationChannels;
        }
    }

    public static class BeamSplitter {
        @SerializedName("BEAMSPLITTER_ENTRY_NAME") private String name;
        @SerializedName("BEAMSPLITTER_ENTRY_FILTER") private String filter;
        @SerializedName("BEAMSPLITTER_ENTRY_FILTER_SET") private String filterSet;
        public String getName() {
            return name;
        }
        public String getFilter() {
            return filter;
        }
        public String getFilterSet() {
            return filterSet;
        }
    }
    
    public static class DataChannel {
        @SerializedName("DATACHANNEL_ENTRY_ACQUIRE") private String acquire;
        @SerializedName("DATACHANNEL_ENTRY_BITSPERSAMPLE") private String bitsPerSample;
        @SerializedName("DATACHANNEL_ENTRY_COLOR") private String color;
        @SerializedName("DATACHANNEL_ENTRY_NAME") private String name;
        @SerializedName("DATACHANNEL_ENTRY_RATIO_CHANNEL1") private String ratioChannel1;
        @SerializedName("DATACHANNEL_ENTRY_RATIO_CHANNEL2") private String ratioChannel2;
        @SerializedName("DATACHANNEL_ENTRY_RATIO_TRACK1") private String ratioTrack1;
        @SerializedName("DATACHANNEL_ENTRY_RATIO_TRACK2") private String ratioTrack2;
        @SerializedName("DATACHANNEL_ENTRY_RATIO_TYPE") private String ratioType;
        @SerializedName("DATACHANNEL_ENTRY_SAMPLETYPE") private String sampleType;
        @SerializedName("DATACHANNEL_MMF_INDEX") private String mmfIndex;
        public String getAcquire() {
            return acquire;
        }
        public String getBitsPerSample() {
            return bitsPerSample;
        }
        public String getColor() {
            return color;
        }
        public String getName() {
            return name;
        }
        public String getRatioChannel1() {
            return ratioChannel1;
        }
        public String getRatioChannel2() {
            return ratioChannel2;
        }
        public String getRatioTrack1() {
            return ratioTrack1;
        }
        public String getRatioTrack2() {
            return ratioTrack2;
        }
        public String getRatioType() {
            return ratioType;
        }
        public String getSampleType() {
            return sampleType;
        }
        public String getMmfIndex() {
            return mmfIndex;
        }
    }

    public static class DetectionChannel {
        @SerializedName("DETCHANNEL_AMPLIFIER_NAME") private String amplifierName;
        @SerializedName("DETCHANNEL_DETECTION_CHANNEL_NAME") private String name;
        @SerializedName("DETCHANNEL_ENTRY_ACQUIRE") private String acquire;
        @SerializedName("DETCHANNEL_ENTRY_DYE_FOLDER") private String dyeFolder;
        @SerializedName("DETCHANNEL_ENTRY_DYE_NAME") private String dyeName;
        @SerializedName("DETCHANNEL_FILTER_SET_NAME") private String filterSetName;
        @SerializedName("DETCHANNEL_INTEGRATOR_NAME") private String integratorName;
        @SerializedName("DETCHANNEL_PINHOLE_NAME") private String pinholeName;
        @SerializedName("DETCHANNEL_POINT_DETECTOR_NAME") private String pointDetectorName;
        @SerializedName("DETCHANNEL_ENTRY_SPI_WAVELENGTH_START") private String wavelengthStart;
        @SerializedName("DETCHANNEL_ENTRY_SPI_WAVELENGTH_END") private String wavelengthEnd;
        @SerializedName("DETCHANNEL_SPI_WAVELENGTH_START2") private String wavelengthStart2;
        @SerializedName("DETCHANNEL_SPI_WAVELENGTH_END2") private String wavelengthEnd2;
        @SerializedName("DETCHANNEL_ENTRY_DETECTOR_GAIN") private String detectorGain;
        @SerializedName("DETCHANNEL_ENTRY_DETECTOR_GAIN_BC1") private String detectorGainBc1;
        @SerializedName("DETCHANNEL_ENTRY_DETECTOR_GAIN_BC2") private String detectorGainBc2;
        @SerializedName("DETCHANNEL_ENTRY_DETECTOR_GAIN_LAST") private String detectorGainLast;
        public String getAmplifierName() {
            return amplifierName;
        }
        public String getName() {
            return name;
        }
        public String getAcquire() {
            return acquire;
        }
        public String getDyeFolder() {
            return dyeFolder;
        }
        public String getDyeName() {
            return dyeName;
        }
        public String getFilterSetName() {
            return filterSetName;
        }
        public String getIntegratorName() {
            return integratorName;
        }
        public String getPinholeName() {
            return pinholeName;
        }
        public String getPointDetectorName() {
            return pointDetectorName;
        }
        public String getWavelengthStart() {
            return wavelengthStart;
        }
        public String getWavelengthEnd() {
            return wavelengthEnd;
        }
        public String getWavelengthStart2() {
            return wavelengthStart2;
        }
        public String getWavelengthEnd2() {
            return wavelengthEnd2;
        }
        public String getDetectorGain() {
            return detectorGain;
        }
        public String getDetectorGainBc1() {
            return detectorGainBc1;
        }
        public String getDetectorGainBc2() {
            return detectorGainBc2;
        }
        public String getDetectorGainLast() {
            return detectorGainLast;
        }
    }
    
    public static class IlluminationChannel {
        @SerializedName("ILLUMCHANNEL_ENTRY_ACQUIRE") private String acquire;
        @SerializedName("ILLUMCHANNEL_ENTRY_NAME") private String name;
        @SerializedName("ILLUMCHANNEL_ENTRY_POWER") private String power;
        @SerializedName("ILLUMCHANNEL_ENTRY_POWER_BC1") private String powerBc1;
        @SerializedName("ILLUMCHANNEL_ENTRY_POWER_BC2") private String powerBc2;
        @SerializedName("ILLUMCHANNEL_ENTRY_WAVELENGTH") private String wavelength;
        public String getAcquire() {
            return acquire;
        }
        public String getName() {
            return name;
        }
        public String getPower() {
            return power;
        }
        public String getPowerBc1() {
            return powerBc1;
        }
        public String getPowerBc2() {
            return powerBc2;
        }
        public String getWavelength() {
            return wavelength;
        }
    }
    
    public static LSMMetadata fromFile(File file) throws IOException {
        Gson gson = new Gson();
        String json = FileUtils.readFileToString(file);
        LSMMetadata zm = gson.fromJson(json, LSMMetadata.class);  
        return zm;
    }

    public static void main(String[] args) throws Exception {

        File file = new File("testfiles/flpo5.json");
        LSMMetadata zm = fromFile(file);
        
        int c = 0;
        for(Channel channel : zm.getChannels()) {
            System.out.println("Channel "+c+" ("+channel.getName()+"): "+channel.getColor());
            if (zm.getDataChannel(channel)!=null) {
                System.out.println("  Data Channel Color: "+zm.getDataChannel(channel).getColor());
            }
            if (zm.getDetectionChannel(channel) != null) {
                System.out.println("  Detection Channel Dye: "+zm.getDetectionChannel(channel).getDyeName());
            }
            System.out.println();
            c++;
        }
        
        for(Laser laser : zm.getLasers()) {
            System.out.println("Laser "+laser.getName()+": power="+laser.getPower());
        }
        System.out.println();
        
        for(Track track : zm.getTracks()) {
            System.out.println(track.getName()+":");
            System.out.println("  multiplexOrder="+track.getMultiplexOrder());
            System.out.println("  isBleach="+track.getIsBleachTrack());
            
            if ("0".equals(track.getIsBleachTrack())) {
                System.out.println("  spiCenterWavelength="+track.getSpiCenterWavelength());
                for(DataChannel dataChannel : track.getDataChannels()) {
                    System.out.println("  Data Channel "+dataChannel.getName()+": color="+dataChannel.getColor()+" bitsPerSample="+dataChannel.getBitsPerSample());    
                }
                for(IlluminationChannel illChannel : track.getIlluminationChannels()) {
                    System.out.println("  Illumination Channel "+illChannel.getName()+": wavelength="+illChannel.getWavelength()+" power="+illChannel.getPower());    
                }
                for(DetectionChannel detChannel : track.getDetectionChannels()) {
                    System.out.println("  Detection Channel "+detChannel.getName()+": dye="+detChannel.getDyeName()+" wavelengthRange=("+detChannel.getWavelengthStart()+", "+detChannel.getWavelengthEnd()+")");    
                }
            }
            
            System.out.println();
        }
        
    }

}
