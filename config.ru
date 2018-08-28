# frozen_string_literal: true

require 'sinatra/cyclist'
require 'dashing'

require 'sequel'
# TODO: use config for connection string.
DBUD = Sequel.connect('postgres://postgres:postgres@localhost/dunbrody')
# DBKR = Sequel.connect('postgres://postgres:postgres@localhost/kromco')

# Which packhouses to query for data:
PACKHOUSES_TO_USE = %w[PH2 PH3].freeze

configure do
  set :auth_token, 'YOUR_AUTH_TOKEN'

  # THIS IS HERE so that we can load the dashboard in an iframe...
  set :protection, except: :frame_options

  # See http://www.sinatrarb.com/intro.html > Available Template Languages on
  # how to add additional template languages.
  set :template_languages, %i[html erb]

  helpers do
    def protected!
      # Put any authentication code you want in here.
      # This method is run before accessing any resource.
    end
  end
end

map Sinatra::Application.assets_prefix do
  run Sinatra::Application.sprockets
end

# set :routes_to_cycle_through, %i[crates_shift_ph3
#                                  rejected_ph3
#                                  rejections_ph3
#                                  running_hours_ph3]
set :routes_to_cycle_through, %i[crates_shift_1_ph2
                                 rejections_ph2]

run Sinatra::Application
