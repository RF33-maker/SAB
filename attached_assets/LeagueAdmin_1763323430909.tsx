import { useEffect, useState } from "react";
import { useLocation, useParams } from "wouter";
import { supabase } from "@/lib/supabase";
import { TeamLogoUploader } from "@/components/TeamLogoUploader";
import { TeamLogo } from "@/components/TeamLogo";
import SwishLogo from "@/assets/Swish Assistant Logo.png";
import UploadSection from "@/components/LeagueAdmin/upload-section-la";

interface League {
  league_id: string;
  name: string;
  slug: string;
  banner_url?: string;
  instagram_embed_url?: string;
  created_by: string;
  user_id: string;
}

interface TeamLogo {
  id: number;
  leagueId: string;
  teamName: string;
  logoUrl: string;
  uploadedBy: string;
  createdAt: string;
  updatedAt: string;
}

export default function LeagueAdmin() {
  const { slug } = useParams();
  const [location, navigate] = useLocation();
  const [league, setLeague] = useState<League | null>(null);
  const [currentUser, setCurrentUser] = useState<any>(null);
  const [isOwner, setIsOwner] = useState(false);
  const [loading, setLoading] = useState(true);
  const [uploadingBanner, setUploadingBanner] = useState(false);
  const [instagramUrl, setInstagramUrl] = useState("");
  const [updatingInstagram, setUpdatingInstagram] = useState(false);
  const [teams, setTeams] = useState<string[]>([]);
  const [teamLogos, setTeamLogos] = useState<Record<string, string>>({});
  const [uploadingLogo, setUploadingLogo] = useState<string | null>(null);
  const [userLeagues, setUserLeagues] = useState<League[]>([]);

  useEffect(() => {
    checkUser();
    if (slug) {
      fetchLeague();
    }
  }, [slug]);

  useEffect(() => {
    if (currentUser) {
      fetchUserLeagues();
    }
  }, [currentUser]);

  // Fetch teams after league is loaded
  useEffect(() => {
    if (league?.league_id) {
      fetchTeams();
    }
  }, [league?.league_id]);

  // Fetch logos after teams are loaded
  useEffect(() => {
    if (league?.league_id && teams.length > 0) {
      fetchTeamLogos();
    }
  }, [league?.league_id, teams.length]);

  const checkUser = async () => {
    try {
      const { data: { user } } = await supabase.auth.getUser();
      setCurrentUser(user);
    } catch (error) {
      console.error("Error checking user:", error);
    }
  };

  const fetchUserLeagues = async () => {
    if (!currentUser) return;
    
    try {
      const { data, error } = await supabase
        .from('leagues')
        .select('*')
        .eq('user_id', currentUser.id)
        .order('created_at', { ascending: false });

      if (error) {
        console.error('Error fetching user leagues:', error);
        return;
      }

      setUserLeagues(data || []);
    } catch (error) {
      console.error('Error fetching user leagues:', error);
    }
  };

  const fetchLeague = async () => {
    try {
      setLoading(true);
      
      const { data: leagues, error } = await supabase
        .from('leagues')
        .select('*')
        .eq('slug', slug)
        .single();

      if (error) {
        console.error("Error fetching league:", error);
        return;
      }

      setLeague(leagues);
      setInstagramUrl(leagues?.instagram_embed_url || "");
      
      // Check if current user is owner
      const { data: { user } } = await supabase.auth.getUser();
      setIsOwner(user?.id === leagues?.created_by || user?.id === leagues?.user_id);
      
    } catch (error) {
      console.error("Error fetching league:", error);
    } finally {
      setLoading(false);
    }
  };

  const fetchTeams = async () => {
    if (!league?.league_id) {
      console.log("No league_id available for fetching teams");
      return;
    }
    
    console.log("Fetching teams for league:", league.league_id);
    
    try {
      // Get all team data from team_stats table (which has proper team names)
      const result = await supabase
        .from('team_stats')
        .select('name')
        .eq('league_id', league.league_id);
        
      console.log("Team query result:", result);
      
      if (result.error) {
        console.error("Error in team query:", result.error);
        setTeams([]);
        return;
      }
      
      if (!result.data || result.data.length === 0) {
        console.log("No team data in team_stats, trying game_schedule...");
        
        // Fallback to game_schedule table
        const scheduleResult = await supabase
          .from('game_schedule')
          .select('hometeam, awayteam')
          .eq('league_id', league.league_id);
        
        console.log("Schedule query result:", scheduleResult);
        
        if (scheduleResult.error) {
          console.error("Error in schedule query:", scheduleResult.error);
          setTeams([]);
          return;
        }
        
        if (!scheduleResult.data || scheduleResult.data.length === 0) {
          console.log("No schedule data found either");
          setTeams([]);
          return;
        }
        
        // Extract unique team names from home and away teams
        const teamNames = new Set<string>();
        scheduleResult.data.forEach((game: any) => {
          if (game.hometeam) teamNames.add(game.hometeam);
          if (game.awayteam) teamNames.add(game.awayteam);
        });
        
        const uniqueTeams = Array.from(teamNames).sort();
        console.log("Unique teams extracted from schedule:", uniqueTeams);
        setTeams(uniqueTeams);
        return;
      }
      
      // Get unique team names from the data
      const uniqueTeams = Array.from(new Set(
        result.data
          .map((stat: any) => stat.name)
          .filter(Boolean)
      ));
      
      console.log("Unique teams extracted:", uniqueTeams);
      setTeams(uniqueTeams);
    } catch (error) {
      console.error("Error fetching teams:", error);
      setTeams([]);
    }
  };

  const fetchTeamLogos = async () => {
    if (!league?.league_id || teams.length === 0) {
      console.log("Skipping logo fetch - no league ID or teams");
      return;
    }
    
    console.log("Fetching logos for teams:", teams);
    
    try {
      // For now, we'll store logos in a simple format
      // Team logos will be stored as files in Supabase storage with predictable names
      const logoMap: Record<string, string> = {};
      
      // Check for existing logo files for each team
      for (const teamName of teams) {
        try {
          const fileName = `${league.league_id}_${teamName.replace(/\s+/g, '_')}.png`;
          console.log(`Checking for logo: ${fileName}`);
          
          const { data } = supabase.storage
            .from('team-logos')
            .getPublicUrl(fileName);
          
          // Check if file exists by trying to fetch it
          const response = await fetch(data.publicUrl, { method: 'HEAD' });
          if (response.ok) {
            console.log(`Found logo for ${teamName}: ${data.publicUrl}`);
            logoMap[teamName] = data.publicUrl;
          } else {
            console.log(`No logo found for ${teamName}`);
          }
        } catch (error) {
          // File doesn't exist, that's okay
          console.log(`Error checking logo for ${teamName}:`, error);
        }
      }
      
      setTeamLogos(logoMap);
      console.log("Team logos loaded:", logoMap);
    } catch (error) {
      console.error("Error fetching team logos:", error);
    }
  };

  const handleBannerUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !league || !currentUser) return;

    setUploadingBanner(true);
    try {
      const fileExt = file.name.split('.').pop();
      const fileName = `${league.league_id}_${Date.now()}.${fileExt}`;
      
      const { data, error } = await supabase.storage
        .from('league-banners')
        .upload(fileName, file);

      if (error) throw error;

      const { data: { publicUrl } } = supabase.storage
        .from('league-banners')
        .getPublicUrl(fileName);

      const { error: updateError } = await supabase
        .from('leagues')
        .update({ banner_url: publicUrl })
        .eq('league_id', league.league_id);

      if (updateError) throw updateError;

      setLeague({ ...league, banner_url: publicUrl });
      
    } catch (error) {
      console.error("Error uploading banner:", error);
    } finally {
      setUploadingBanner(false);
    }
  };

  const handleInstagramUpdate = async () => {
    if (!league || !currentUser) return;

    setUpdatingInstagram(true);
    try {
      const { error } = await supabase
        .from('leagues')
        .update({ instagram_embed_url: instagramUrl })
        .eq('league_id', league.league_id);

      if (error) throw error;

      setLeague({ ...league, instagram_embed_url: instagramUrl });
      
    } catch (error) {
      console.error("Error updating Instagram URL:", error);
    } finally {
      setUpdatingInstagram(false);
    }
  };

  const handleTeamLogoUpload = async (teamName: string, file: File) => {
    if (!league || !currentUser) return;

    setUploadingLogo(teamName);
    try {
      // Upload to Supabase storage with predictable filename
      const fileExt = file.name.split('.').pop();
      const fileName = `${league.league_id}_${teamName.replace(/\s+/g, '_')}.${fileExt}`;
      
      console.log("Uploading file:", fileName);
      
      // Upload directly to Supabase storage
      const { data, error } = await supabase.storage
        .from('team-logos')
        .upload(fileName, file, {
          upsert: true // This will overwrite existing files
        });

      if (error) {
        console.error("Upload error:", error);
        throw error;
      }

      console.log("Upload successful:", data);

      // Get public URL
      const { data: { publicUrl } } = supabase.storage
        .from('team-logos')
        .getPublicUrl(fileName);

      console.log("Public URL:", publicUrl);

      // Update local state
      setTeamLogos(prev => ({
        ...prev,
        [teamName]: publicUrl
      }));
      
      alert('Team logo uploaded successfully!');
    } catch (error) {
      console.error("Error uploading team logo:", error);
      alert(`Failed to upload team logo: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setUploadingLogo(null);
    }
  };

  const handleRemoveLogo = async (teamName: string) => {
    if (!league || !currentUser) return;

    try {
      // Remove from Supabase storage
      const fileName = `${league.league_id}_${teamName.replace(/\s+/g, '_')}.png`;
      const { error } = await supabase.storage
        .from('team-logos')
        .remove([fileName]);

      if (error) throw error;

      // Update local state
      setTeamLogos(prev => {
        const updated = { ...prev };
        delete updated[teamName];
        return updated;
      });
      
      alert('Team logo removed successfully!');
    } catch (error) {
      console.error("Error removing team logo:", error);
      alert('Failed to remove team logo');
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-orange-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600">Loading league admin...</p>
        </div>
      </div>
    );
  }

  if (!league) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-800 mb-2">League Not Found</h1>
          <p className="text-gray-600 mb-4">The league you're looking for doesn't exist.</p>
          <button
            onClick={() => navigate('/')}
            className="bg-orange-500 hover:bg-orange-600 text-white px-4 py-2 rounded-lg"
          >
            Go Home
          </button>
        </div>
      </div>
    );
  }

  if (!isOwner) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-800 mb-2">Access Denied</h1>
          <p className="text-gray-600 mb-4">You don't have permission to access this league's admin panel.</p>
          <button
            onClick={() => navigate(`/league/${slug}`)}
            className="bg-orange-500 hover:bg-orange-600 text-white px-4 py-2 rounded-lg"
          >
            View League
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-full md:max-w-7xl mx-auto px-4 md:px-6 py-4 md:py-6">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <button
                  onClick={() => navigate(`/league/${slug}`)}
                  className="text-gray-600 hover:text-gray-800"
                  data-testid="button-back-to-league"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                  </svg>
                </button>
                <div>
                  <h1 className="text-xl md:text-2xl font-bold text-gray-800">League Admin</h1>
                  <p className="text-sm md:text-base text-gray-600">{league.name}</p>
                </div>
              </div>
              <img src={SwishLogo} alt="Swish" className="h-6 md:h-8 md:hidden" />
            </div>
            <div className="flex items-center gap-4">
              <button
                onClick={() => navigate('/league-management')}
                className="flex items-center gap-2 w-full md:w-auto justify-center px-4 md:px-4 py-2 text-sm md:text-base text-orange-600 hover:text-orange-700 hover:bg-orange-50 rounded-lg transition-colors border border-orange-200 hover:border-orange-300"
                data-testid="button-league-management"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
                </svg>
                <span className="font-medium">League Management</span>
              </button>
              <img src={SwishLogo} alt="Swish" className="hidden md:block h-8" />
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-full md:max-w-7xl mx-auto px-4 md:px-6 py-4 md:py-8">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
          
          {/* Banner Management */}
          <div className="bg-white rounded-xl shadow p-4 md:p-6">
            <div className="flex items-center gap-3 mb-4 md:mb-6">
              <div className="w-10 h-10 bg-orange-100 rounded-lg flex items-center justify-center">
                <svg className="w-5 h-5 text-orange-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
                </svg>
              </div>
              <div>
                <h2 className="text-lg md:text-xl font-semibold text-gray-800">League Banner</h2>
                <p className="text-xs md:text-sm text-gray-600">Upload a custom banner for your league page</p>
              </div>
            </div>

            {league.banner_url && (
              <div className="mb-4">
                <img 
                  src={league.banner_url} 
                  alt="League Banner" 
                  className="w-full h-32 md:h-48 object-cover rounded-lg border"
                />
              </div>
            )}

            <input
              type="file"
              accept="image/*"
              onChange={handleBannerUpload}
              className="hidden"
              id="banner-upload"
              disabled={uploadingBanner}
            />
            <label
              htmlFor="banner-upload"
              className={`w-full inline-flex items-center justify-center gap-2 px-4 md:px-6 py-2 md:py-3 text-sm md:text-base bg-orange-500 hover:bg-orange-600 text-white font-medium rounded-lg cursor-pointer transition-colors ${
                uploadingBanner ? 'opacity-50 cursor-not-allowed' : ''
              }`}
            >
              {uploadingBanner ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                  Uploading...
                </>
              ) : (
                <>
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                  </svg>
                  {league.banner_url ? 'Change Banner' : 'Upload Banner'}
                </>
              )}
            </label>
          </div>

          {/* Instagram Management */}
          <div className="bg-white rounded-xl shadow p-4 md:p-6">
            <div className="flex items-center gap-3 mb-4 md:mb-6">
              <div className="w-10 h-10 bg-pink-100 rounded-lg flex items-center justify-center">
                <svg className="w-5 h-5 text-pink-600" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/>
                </svg>
              </div>
              <div>
                <h2 className="text-lg md:text-xl font-semibold text-gray-800">Instagram Integration</h2>
                <p className="text-xs md:text-sm text-gray-600">Connect your league's Instagram profile</p>
              </div>
            </div>

            <div className="flex flex-col md:flex-row gap-3 md:gap-4">
              <input
                type="text"
                placeholder="Enter Instagram profile URL (e.g., https://www.instagram.com/yourleague)"
                value={instagramUrl}
                onChange={(e) => setInstagramUrl(e.target.value)}
                className="w-full px-3 md:px-3 py-2 md:py-3 text-sm md:text-base border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange-500 focus:border-transparent"
              />
              <button
                onClick={handleInstagramUpdate}
                disabled={updatingInstagram}
                className={`w-full md:w-auto px-4 md:px-6 py-2 md:py-3 text-sm md:text-base font-medium rounded-lg transition-colors ${
                  updatingInstagram 
                    ? 'bg-gray-300 text-gray-500 cursor-not-allowed' 
                    : 'bg-pink-500 text-white hover:bg-pink-600'
                }`}
              >
                {updatingInstagram ? 'Updating...' : 'Update Instagram'}
              </button>
            </div>
          </div>

          {/* PDF Upload Section */}
          <div className="md:col-span-2 bg-white rounded-xl shadow overflow-hidden">
            <UploadSection leagues={userLeagues} />
          </div>

          {/* Team Logo Management */}
          <div className="md:col-span-2 bg-white rounded-xl shadow p-4 md:p-6">
            <div className="flex items-center gap-3 mb-4 md:mb-6">
              <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                <svg className="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
                </svg>
              </div>
              <div>
                <h2 className="text-lg md:text-xl font-semibold text-gray-800">Team Logo Management</h2>
                <p className="text-xs md:text-sm text-gray-600">Upload and manage logos for all teams in your league</p>
              </div>
            </div>

            <div className="mb-4">
              <p className="text-sm text-gray-600">Found {teams.length} teams in this league</p>
            </div>

            {teams.length > 0 ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-2 lg:grid-cols-3 gap-4 md:gap-6">
                {teams.map((teamName) => (
                  <div key={teamName} className="border border-gray-200 rounded-lg p-3 md:p-4">
                    <div className="text-center mb-4">
                      <TeamLogo 
                        teamName={teamName} 
                        leagueId={league.league_id} 
                        size="lg" 
                        className="mx-auto mb-2" 
                      />
                      <h3 className="font-medium text-gray-800">{teamName}</h3>
                    </div>

                    <div className="space-y-2">
                      <input
                        type="file"
                        accept="image/*"
                        onChange={(e) => {
                          const file = e.target.files?.[0];
                          if (file) handleTeamLogoUpload(teamName, file);
                        }}
                        className="hidden"
                        id={`logo-upload-${teamName}`}
                        disabled={uploadingLogo === teamName}
                      />
                      <label
                        htmlFor={`logo-upload-${teamName}`}
                        className={`w-full inline-flex items-center justify-center gap-2 px-3 md:px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white text-xs md:text-sm rounded-lg cursor-pointer transition-colors ${
                          uploadingLogo === teamName ? 'opacity-50 cursor-not-allowed' : ''
                        }`}
                      >
                        {uploadingLogo === teamName ? (
                          <>
                            <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                            Uploading...
                          </>
                        ) : (
                          <>
                            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                            </svg>
                            Upload Logo
                          </>
                        )}
                      </label>
                      
                      {teamLogos[teamName] && (
                        <button
                          onClick={() => handleRemoveLogo(teamName)}
                          className="w-full px-3 py-2 text-sm text-red-600 hover:text-red-700 border border-red-200 hover:border-red-300 rounded-lg transition-colors"
                        >
                          Remove Logo
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-12 text-gray-500">
                <svg className="w-16 h-16 mx-auto mb-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
                </svg>
                <p className="text-lg font-medium mb-2">No Teams Found</p>
                <p className="text-sm">Teams will appear here once player data is added to your league.</p>
              </div>
            )}
          </div>

        </div>
      </main>
    </div>
  );
}