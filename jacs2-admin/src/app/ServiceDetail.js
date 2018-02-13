import React, { Component } from 'react';
import { connect } from 'react-redux';
import Heading from 'grommet/components/Heading';
import Section from 'grommet/components/Section';
import List from 'grommet/components/List';
import ListItem from 'grommet/components/ListItem';
import Paragraph from 'grommet/components/Paragraph';
import FormField from 'grommet/components/FormField';
import TextInput from 'grommet/components/TextInput';
import Label from 'grommet/components/Label';
import { fetchServices } from './actions'

class ServiceDetail extends Component {
  componentDidMount() {
      this.props.fetchServices('http://jacs2.int.janelia.org:9000');
  }

  render() {
      return (
           <Section>
               <Heading>Wonderful Service</Heading>
               <Paragraph>This is a description</Paragraph>
               <Heading tag="h3">Parameters</Heading>
               <List>
                   <ListItem>
                       <Label>Item 1</Label>
                       <FormField>
                       <TextInput id='item1'
                                  name='item-1'
                                  value='one'
                                 suggestions={['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight']} />
                        </FormField>
                    </ListItem>
               </List>
           </Section>

        );
  }
}

const mapStateToProps = (state) => {
    return {
        services: state.servicesRegistry.services
    };
};
const mapDispatchToProps = (dispatch) => {
    return {
        fetchServices: (url) => dispatch(fetchServices(url))
    };
};

export default connect(mapStateToProps, mapDispatchToProps)(ServiceDetail);